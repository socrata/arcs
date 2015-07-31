import sys
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


def _query_counts_df(df):
    return df["query"].value_counts()


def get_top_queries(df):
    """Get the most frequently occurring query terms irrespective of domain."""

    df = df[pd.notnull(df["query"])]

    return df["query"].value_counts


def get_public_domains(db_conn_str):
    """Determine public domains from metadb."""

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


def sample_domains(df, n=10, min_query_count=10):
    """Get a a weighted (by count) sample of domains."""

    domain_counts = df["domain"].value_counts()
    df = pd.DataFrame({"domain": domain_counts.keys(),
                       "count": domain_counts.values})

    df = df[df["count"] >= min_query_count]

    weights = df["count"] / df["count"].sum()

    return {x for x in df.sample(n=min(n, len(df)), weights=weights)["domain"]}


def sample_queries_by_domain(df, num_domains, num_queries, min_uniq_terms=10):
    """Get the most frequently occurring query terms grouped by domain."""

    # get a weighted sample of domains
    domains = sample_domains(df, num_domains, num_queries)

    # group by domain, split, and get query counts
    by_domain = df.groupby("domain")

    # get per-domain query counts
    domain_dfs = {d: _query_counts_df(by_domain.get_group(d))
                  for d in by_domain.groups if d in domains}

    # filter to only domains w/ min_uniq_terms query terms or more
    domain_dfs = {d: counts for d, counts in domain_dfs.items()
                  if len(counts) >= min_uniq_terms}

    # for each domain, sample n queries proportional to query frequency
    return list(chain.from_iterable([[
        (d, q) for q in counts.sample(
            n=min(num_queries, len(counts)),
            weights=(counts / counts.sum())).index.tolist()]
                                    for d, counts in domain_dfs.items()]))


def lang_filter(s):
    try:
        return ldetect(s) == 'en'
    except Exception:
        return False


def get_cetera_results(domain_query_pairs, cetera_host=None, cetera_port=None,
                       num_results=None):
    """
    Get the top n=num_results catalog search results from Cetera for each
    (domain, query) pair in domain_query_pairs.
    """
    cetera_host = cetera_host or "http://localhost"
    cetera_port = cetera_port or 5704
    num_results = num_results or 10

    #url = "{}:{}".format(cetera_host, cetera_port)
    url = cetera_host
    params = frozendict({"limit": num_results})

    def _get_result_list(domain, query):
        print domain, query
        r = requests.get(url, params=params.copy(domains=domain, q=query))
        return [res for res in list(enumerate(r.json()["results"]))
                if lang_filter(res[1]['resource'].get('description'))][:10]

    return [(d, q, _get_result_list(d, q)) for d, q in domain_query_pairs]


def _transform_cetera_result(result):
    """
    Utility function for transforming Cetera result dictionary into something
    more suitable for the crowdsourcing task. Presently, we're grabbing name,
    link (ie. URL), the first sentence of description, and the updatedAt
    timestamp.
    """
    desc = result["resource"].get("description").replace("\r", "\n")
    desc_sentences = desc.split("\n") if desc else []
    desc = desc_sentences[0] if desc_sentences else desc

    return (result["resource"].get("name"),
            result["link"],
            desc,
            result["resource"].get("updatedAt"))

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
    bundle everything up and write out as a CSV.
    """
    assert(num_results > 0)

    output_file = output_file or "{}.csv".format(datetime.now().strftime("%Y%m%d"))

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

    domain_queries = sample_queries_by_domain(df, num_domains, queries_per_domain)

    logging.info("Getting search results from Cetera")

    results = get_cetera_results(domain_queries, cetera_host=cetera_host,
                                 cetera_port=cetera_port, num_results=num_results)

    logging.info("Fetching domain logos")

    domains = {x[0] for x in domain_queries}
    logos = {domain: get_domain_image(domain) for domain in domains}

    results = pd.DataFrame(
        list(chain.from_iterable(
            [[(d, logos.get(d), q, r[0]) + _transform_cetera_result(r[1]) for r in rs]
             for d, q, rs in results])),
        columns=CSV_COLUMNS)

    logging.info("Writing out results as CSV")

    results.to_csv(output_file, encoding="utf-8", index=False, escapechar="\\", na_rep=None)


if __name__ == "__main__":
    collect_task_data(sys.argv[1],
                      10, 10, 10,
                      db_conn_str="postgresql://animl:animl@metadba.sea1.socrata.com:5432/blist_prod",
                      #cetera_host='http://search.cetera.aws-us-west-2-prod.socrata.net',
                      cetera_host='https://api.us.socrata.com/api/catalog/v1',
                      cetera_port='80')
