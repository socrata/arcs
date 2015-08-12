import argparse
import pandas as pd
from pandas import read_sql
from frozendict import frozendict
from sqlalchemy import create_engine
import requests
from itertools import chain
import simplejson
import logging
import os
import re
from datetime import datetime
from logparser import apply_filters
from langdetect import detect as ldetect
from collections import defaultdict


def _query_counts_df(df):
    return df["query"].value_counts()


def get_top_queries(df):
    """Get the most frequently occurring query terms irrespective of domain."""

    df = df[pd.notnull(df["query"])]

    return df["query"].value_counts


def get_public_domains(db_conn_str):
    """
    Determine public domains from metadb.

    It feels a little cheesy to use the Pandas `read_sql` function here, since
    we promptly convert the relevant columns into sets. However, it also seemed
    silly to incorporate another library for interacting w/ SQL when we're
    already making heavy use of Pandas.
    """

    conn = create_engine(db_conn_str)

    domains_df = read_sql("SELECT id, cname as domain FROM domains", con=conn)

    domains = set(domains_df["id"])

    private = set(read_sql(
        "SELECT c.domain_id AS id FROM config_vals AS cv, configurations as c " \
        "WHERE cv.config_id=c.id AND cv.name='staging_api_lockdown' AND " \
        "c.type='feature_set' AND c.is_default AND c.deleted_at IS NULL AND " \
        "cv.deleted_at IS NULL", con=conn)["id"])

    lockdown_false = set(read_sql(
        "SELECT c.domain_id AS id FROM config_vals AS cv, configurations as c " \
        "WHERE cv.config_id=c.id AND cv.name='staging_api_lockdown' AND " \
        "c.type='feature_set' AND c.is_default AND c.deleted_at IS NULL AND " \
        "cv.deleted_at IS NULL AND cv.value='false'", con=conn)["id"])

    public_domains = domains.difference(private).union(lockdown_false)

    return domains_df[domains_df["id"].apply(lambda x: x in public_domains)]


def sample_domains(df, num_domains=10, min_query_count=10):
    """Get a a weighted (by count) sample of domains."""

    domain_counts = df["domain"].value_counts()
    df = pd.DataFrame({"domain": domain_counts.keys(),
                       "count": domain_counts.values})

    df = df[df["count"] >= min_query_count]

    weights = df["count"] / df["count"].sum()

    # only grab len(df) domains if we're asking for more than we have
    return set(df.sample(n=min(num_domains, len(df)), weights=weights)["domain"])


def sample_queries_by_domain(df, num_domains, queries_per_domain,
                             min_uniq_terms=10, domain_buffer_factor=2,
                             query_buffer_factor=2):
    """
    Get the most frequently occurring query terms grouped by domain.

    Initially, get `domain_buffer_factor` as many domains so that we can ignore
    the ones that don't fit our other criteria.

    Args:
        df: A Pandas DataFrame of query log records
        num_domains: The number of domains to sample
        queries_per_domain: The number of queries to sample per domain
        min_uniq_terms: The min. number of unique query terms required for
            a domain to be considered for sampling
        domain_buffer_factor: Factor by which to buffer domain sample
        query_buffer_factor: Factor by which to buffer query sample

    Returns: A list of (domain, query) pairs.
    """
    # get a weighted sample of domains
    domain_buffer = domain_buffer_factor or 1
    domains = sample_domains(df, num_domains * domain_buffer)

    # group by domain, split, and get query counts
    by_domain = df.groupby("domain")

    # get per-domain query counts
    domain_dfs = {d: _query_counts_df(by_domain.get_group(d))
                  for d in by_domain.groups if d in domains}

    # filter to only domains w/ min_uniq_terms query terms or more
    domain_dfs = {d: counts for d, counts in domain_dfs.items()
                  if len(counts) >= min_uniq_terms}

    # for each domain, sample n queries proportional to query frequency
    query_buffer = query_buffer_factor or 1
    return list(chain.from_iterable([[
        (d, q) for q in counts.sample(
            n=min(queries_per_domain * query_buffer, len(counts)),
            weights=(counts / counts.sum())).index.tolist()]
        for d, counts in domain_dfs.items()]))


def lang_filter(s):
    try:
        return ldetect(s) == 'en'
    except Exception:
        return False


def get_cetera_results(domain_query_pairs, cetera_host="http://localhost", cetera_port=None,
                       num_results=10, queries_per_domain=10):
    """
    Get the top n=num_results catalog search results from Cetera for each
    (domain, query) pair in domain_query_pairs.
    """
    # we can't use the port in this version
    if 'https://api.us.socrata.com/api/catalog/' in cetera_host:
        url = cetera_host
    else:
        url = "{}:{}".format(cetera_host, cetera_port)
    # multiply by two because we're going to langfilter
    params = frozendict({"limit": num_results*2})

    def _get_result_list(domain, query):
        print domain, query
        r = requests.get(url, params=params.copy(domains=domain, q=query))
        return [res for res in list(enumerate(r.json().get("results")))
                if lang_filter(res[1]['resource'].get('description'))][:num_results]

    res = [(d, q, _get_result_list(d, q)) for d, q in domain_query_pairs]
    # filter for only the (d, q, result_list) tuples that have at least num_results results
    filtered = [(d, q, rl) for d, q, rl in res if len(rl) >= num_results]
    # filter for only the domains that have at least queries_per_domain queries
    # totally gross, but it works
    dom_counts = defaultdict(set)
    [dom_counts[d].add(q) for d, q, rl in filtered]
    dom_counts_limited = {d: list(q)[:queries_per_domain] for d, q in dom_counts.iteritems()}
    filtered = [(d, q, rl) for d, q, rl in filtered if len(dom_counts_limited[d]) >= queries_per_domain and q in dom_counts_limited[d]]
    return filtered


def _transform_cetera_result(result):
    """
    Utility function for transforming Cetera result dictionary into something
    more suitable for the crowdsourcing task. Presently, we're grabbing name,
    link (ie. URL), and the first sentence of description.
    """
    desc = result["resource"].get("description").replace("\r", "\n")
    desc_sentences = desc.split("\n") if desc else []
    desc = desc_sentences[0] if desc_sentences else desc

    return (result["resource"].get("name"),
            result["link"],
            desc)

_LOGO_UID_RE = re.compile(r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$")


def get_domain_image(domain):
    """Get the site logo for the specified domain."""
    url = 'http://{0}/api/configurations.json'.format(domain)

    params = {
        'type': 'site_theme',
        'defaultOnly': True,
        'merge': True
    }

    response = None

    try:
        response = requests.get(url, params=params, timeout=(5, 10))
        response.encoding = 'utf-8'
        response.raise_for_status()

        data = next((x for x in response.json()[0]["properties"]
                     if x.has_key("name") and x["name"] == "theme_v2b"))

        url = data.get("value", {}).get("images", {}).get("logo_header", {}).get("href")

        if url and _LOGO_UID_RE.match(url):
            url = "/api/assets/{0}".format(url)

        if not (url.startswith("http") or url.startswith("https")):
            url = "http://{0}{1}".format(domain, url)
        return url

    except IndexError as e:
        print "Unexpected result shape: zero elements in response JSON"
        print "Response: {}".format(response.content if response else None)
        print "Exception: {}".format(e.message)
    except StopIteration as e:
        print "Unable to find image properties in response JSON"
        print "Response: {}".format(response.content if response else None)
        print "Exception: {}".format(e.message)
    except Exception as e:
        print "Failed to fetch configuration for %s" % domain
        print "Response: %s" % response.content if response else None
        print "Exception: %s" % e.message


CSV_COLUMNS = ['domain', 'domain logo url', 'query', 'result position',
               'name', 'link', 'description', 'updatedAt']


def collect_task_data(query_logs_json, num_domains, queries_per_domain,
                      num_results, output_file=None, cetera_host=None,
                      cetera_port=None, db_conn_str=None):
    """
    Do frequency weighted sampling of query logs for domain-specific queries.
    Send those queries as requests to Cetera, collecting n=num_results results
    for each query. For each domain, gather URLs to domain logos. Finally,
    bundle everything up and write out as a CSV. Use Pandas DataFrame as the
    primary data structure for storing the log records.
    """
    assert(num_results > 0)

    output_file = output_file or \
        "{}.csv".format(datetime.now().strftime("%Y%m%d"))

    logging.info("Reading query logs from {}".format(query_logs_json))

    with open(query_logs_json, "r") as f:
        df = pd.DataFrame(
            (x for x in (simplejson.loads(line.strip()) for line in f)
             if apply_filters(x)))

    logging.info("Determining public domains")

    public_domains = get_public_domains(db_conn_str or
                                        os.environ["METADB_CONN_STR"])

    public_domains = set(public_domains["domain"])

    logging.info("Filtering log data to public domains")

    df = df[df["domain"].apply(lambda domain: domain in public_domains)]

    logging.info("Sampling queries by domain")

    domain_queries = sample_queries_by_domain(
        df, num_domains, queries_per_domain)

    logging.info("Getting search results from Cetera")

    results = get_cetera_results(domain_queries, cetera_host=cetera_host,
                                 cetera_port=cetera_port,
                                 num_results=num_results,
                                 queries_per_domain=queries_per_domain)

    logging.info("Fetching domain logos")

    domains = {x[0] for x in domain_queries}
    logos = {domain: get_domain_image(domain) for domain in domains}

    results = pd.DataFrame(
        list(chain.from_iterable([[
            (d, logos.get(d), q, r[0]) + _transform_cetera_result(r[1])
            for r in rs] for d, q, rs in results])),
        columns=CSV_COLUMNS)

    # limit to the first num_domains*queries_per_domain*num_results
    results = results[:num_domains*queries_per_domain*num_results]

    logging.info("Writing out results as CSV")

    results.to_csv(output_file, encoding="utf-8",
                   index=False, escapechar="\\", na_rep=None)


def arg_parser():
    parser = argparse.ArgumentParser(description='Gather domains and queries from parsed nginx logs, \
    gather the top n results from cetera')

    # TODO: make all the inputs to collect_task_data configurable here!
    parser.add_argument('-j', '--json_file', dest='query_logs_json', required=True,
                        help='json file containing the output of logparse.py')
    parser.add_argument('-d', '--num_domains', dest='num_domains', type=int,
                        default=40,
                        help='Number of domains to fetch queries and results for, \
                        default %(default)s')
    parser.add_argument('-q', '--num_queries', dest='queries_per_domain', type=int,
                        default=5,
                        help='Number of queries per domain to fetch, \
                        default %(default)s')
    parser.add_argument('-r', '--num_results', dest='num_results', type=int,
                        default=40,
                        help='Number of results per (domain, query) pair to fetch from cetera, \
                        default %(default)s')
    parser.add_argument('-c', '--cetera_host', dest='cetera_host',
                        default='https://api.us.socrata.com/api/catalog/v1',
                        help='Cetera hostname (eg. localhost) \
                        default %(default)s')
    parser.add_argument('-p', '--cetera_port', dest='cetera_port',
                        default='80',
                        help='Cetera port, default %(default)s')

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    args = arg_parser()
    collect_task_data(args.query_logs_json, args.num_domains,
                      args.queries_per_domain, args.num_results,
                      cetera_host=args.cetera_host,
                      cetera_port=args.cetera_port)
