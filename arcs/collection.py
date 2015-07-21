import pandas as pd
from pandas import read_sql
from frozendict import frozendict
from sqlalchemy import create_engine
import requests
from itertools import chain
import simplejson
import logging
import os
from datetime import datetime
from logparser import apply_filters

def _query_counts_df(df):
    return df["query"].value_counts()

def get_top_queries(df):
    """Get the most frequently occurring query terms irrespective of domain."""

    df = df[pd.notnull(df["query"])]

    return df["query"].value_counts

def get_metadata_from_db(db_conn_str):
    """Read dataset metadata from DB."""

    domains_df = read_sql(
        "SELECT domain_cname AS domain, is_public, obe_fxf, nbe_fxf FROM cetera_fact_metadb",
        con=create_engine(db_conn_str))

    return domains_df

def sample_domains(df, n=10):
    """Get a a weighted (by count) sample of domains."""

    domain_counts = df["domain"].value_counts()
    df = pd.DataFrame({"domain": domain_counts.keys(),
                       "count": domain_counts.values})

    weights = df["count"] / df["count"].sum()

    return {x for x in df.sample(n=n, weights=weights)["domain"]}

def sample_queries_by_domain(df, num_domains, num_queries, min_uniq_terms=10):
    """Get the most frequently occurring query terms grouped by domain."""

    # get a weighted sample of domains
    domains = sample_domains(df, num_domains)

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
            n=num_queries,
            weights=(counts / counts.sum())).index.tolist()]
                                     for d, counts in domain_dfs.items()]))

def get_cetera_results(domain_query_pairs, cetera_host=None, cetera_port=None,
                       num_results=None):
    """
    Get the top n=num_results catalog search results from Cetera for each
    (domain, query) pair in domain_query_pairs.
    """
    cetera_host = cetera_host or "http://localhost"
    cetera_port = cetera_port or 5704
    num_results = num_results or 10

    url = "{}:{}/catalog".format(cetera_host, cetera_port)
    params = frozendict({"limit": num_results})

    def _get_result_list(domain, query):
        return list(enumerate(
            requests.get(
                url, params.copy(domains=domain, search=query)
            ).json()["results"]))

    return [(d, q, _get_result_list(d, q)) for d, q in domain_query_pairs]

def _transform_cetera_result(result):
    """
    Utility function for transforming Cetera result dictionary into something
    more suitable for the crowdsourcing task. Presently, we're grabbing name,
    link (ie. URL), the first sentence of description, and the updatedAt
    timestamp.
    """
    desc = result["resource"].get("description")
    desc_sentences = desc.split("\n") if desc else []
    desc = desc_sentences[0] if desc_sentences else desc

    return (result["link"],
            desc,
            result["resource"].get("updatedAt"),
            result["resource"].get("name"))

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

        return data.get("value", {}).get("images", {}).get("logo_header", {}).get("href")
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

csv_columns=['domain', 'domain logo url', 'query', 'result position',
             'name', 'link', 'description', 'updatedAt']

def collect_task_data(query_logs_json, num_domains, queries_per_domain, num_results,
                      output_file=None, cetera_host=None, cetera_port=None, db_conn_str=None):
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
        columns=csv_columns)

    logging.info("Writing out results as CSV")

    results.to_csv(output_file, encoding="utf-8", index=False)
