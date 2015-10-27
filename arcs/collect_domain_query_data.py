import logging
import re
import simplejson
import pandas as pd
from itertools import chain
from langdetect import detect as ldetect
from logparser import apply_filters

_HAS_LETTERS_RE = re.compile("[A-Za-z]")
_LOOKS_LIKE_CODE_RE = re.compile(
    r"(?:text/javascript|"
    r"select .*? from|"
    r"/etc/passwd|"
    r"systemroot|"
    r"alert\(|"
    r"meta|"
    r"\.ini|"
    r"img|"
    r"\\x[\d]|"
    r"[<>])",
    re.IGNORECASE)


def sample_domains(df, num_domains=10, min_query_count=10):
    """
    Get a a weighted (by count) sample of domains.

    Args:
        df (pandas.DataFrame): A Pandas DataFrame with a domain column

    Returns:
        A sample of domains as a set
    """
    domain_counts = df["domain"].value_counts()
    df = pd.DataFrame({"domain": domain_counts.keys(),
                       "count": domain_counts.values})

    df = df[df["count"] >= min_query_count]

    weights = df["count"] / df["count"].sum()

    # only grab len(df) domains if we're asking for more than we have
    return set(df.sample(n=min(num_domains, len(df)), weights=weights)["domain"])


def get_public_domains(db_conn):
    """
    Determine public domains from metadb.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database: A DB connection string

    Returns:
        A 2-column Pandas DataFrame with domain_id and domain_cname
    """
    domains_df = pd.read_sql("SELECT DISTINCT domain_id, domain_cname FROM "
                             "cetera_core_datatypes_snapshot WHERE is_public = true", con=db_conn)

    return domains_df


def normalize_query(query):
    """
    Perform query normalization.

    At the moment, this is simply downcasing and stripping whitespace from the ends. Extend this as
    needed.

    Args:
        query (str): A query string

    Returns:
        A normalized query string
    """
    return query.lower().strip()


def sample_queries_by_domain(df, num_domains, queries_per_domain,
                             min_uniq_terms=10, domain_buffer_factor=1,
                             query_buffer_factor=None, domains=None,
                             query_blacklist=None):
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
        domains: Optional list of domains for which to sample queries

    Returns:
        A list of (domain, query, count) triples
    """
    # get a weighted sample of domains
    domains = domains or sample_domains(df, num_domains * domain_buffer_factor)

    # group by domain, split, and get query counts
    by_domain = df.groupby("domain")

    # get per-domain query counts
    domain_dfs = {d: by_domain.get_group(d)["query"].value_counts()
                  for d in by_domain.groups if d in domains}

    # filter to only domains w/ min_uniq_terms query terms or more
    domain_dfs = {d: counts for d, counts in domain_dfs.items()
                  if len(counts) >= min_uniq_terms}

    # for each domain, sample n queries proportional to query frequency
    # excluding any queries that are in our blacklist
    query_blacklist = query_blacklist or frozenset()

    query_buffer = query_buffer_factor or 1

    # filter out queries in query blacklist
    domain_dfs = {d: counts[counts.apply(lambda q: q not in query_blacklist)]
                  for d, counts in domain_dfs.items()}

    return list(chain.from_iterable([[
        (d, q, counts.loc[q]) for q in counts.sample(
            n=min(queries_per_domain * query_buffer, len(counts)),
            weights=(counts / counts.sum())).index.tolist()]
        for d, counts in domain_dfs.items()]))


def lang_filter(s):
    res = False

    try:
        res = ldetect(s) == 'en'
    except Exception:
        pass

    return res


def is_well_formed_utf8(s):
    res = False

    try:
        s.encode('utf-8')
        res = True
    except:
        pass

    return res


def query_filter(query_blacklist, q, filters=None):
    return bool(_HAS_LETTERS_RE.search(q)) and \
        q not in query_blacklist and \
        not _LOOKS_LIKE_CODE_RE.search(q) and \
        is_well_formed_utf8(q) and \
        all([fn(q) for fn in filters]) if filters else True


def read_query_blacklist_from_file(f):
    with open(f, "r") as infile:
        return frozenset([x.strip() for x in infile.read()])


def get_domain_query_sample_from_logs(db_conn, query_logs_json, num_domains,
                                      queries_per_domain, query_blacklist_file=None,
                                      query_filters=None):
    """
    Read all queries against public domains from query logs specified by `query_logs_json`,
    sample `num_domains` domains, and `queries_per_domain` queries for each domain. Queries
    are normalized and filtered to remove garbage queries.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database: A DB connection string
        query_logs_json (str): A path to a query log JSON file (the output of logparse.py)
        num_domains (int): The number of domains to sample
        queries_per_domain (int): The number of queries to sample per domain
        query_blacklist_file (str): An optional file containing queries to exclude
        query_filters (iterable): An optional list of filtering functions to apply to each query
            log record

    Returns:
        A list of (domain, query, count) triples, where the count indicates how many times the
        domain-query pair was observed in the logs.
    """
    logging.info("Reading query logs from {}".format(args.query_logs_json))

    with open(query_logs_json, "r") as f:
        df = pd.DataFrame(
            (x for x in (simplejson.loads(line.strip()) for line in f)
             if apply_filters(x)))

    if query_blacklist_file:
        logging.info("Reading query blacklist from {}".format(query_blacklist_file))
        query_blacklist = read_query_blacklist_from_file(query_blacklist_file)
    else:
        query_blacklist = frozenset()

    logging.info("Filtering queries")

    df = df[df["query"].apply(lambda q: query_filter(query_blacklist, q, filters=query_filters))]

    logging.info("Determining public domains")

    public_domains = set(get_public_domains(db_conn)["domain_cname"])

    logging.info("Filtering log data to public domains")

    df = df[df["domain"].apply(lambda domain: domain in public_domains)]

    logging.info("Normalizing queries")

    df["query"] = df["query"].apply(normalize_query)

    logging.info("Sampling queries by domain")

    return sample_queries_by_domain(df, args.num_domains, args.queries_per_domain)


if __name__ == "__main__":
    import argparse
    import psycopg2

    parser = argparse.ArgumentParser(
        description='Gather top domains and queries from parsed nginx logs')

    parser.add_argument('query_logs_json',
                        help='JSON file containing the output of logparse.py')

    parser.add_argument('-D', '--db_conn_str', required=True,
                        help='Database connection string')

    parser.add_argument('-d', '--num_domains', dest='num_domains', type=int,
                        default=10,
                        help='Number of domains to fetch queries and results '
                        'for, default %(default)s')

    parser.add_argument('-q', '--num_queries', dest='queries_per_domain',
                        type=int, default=5,
                        help='Number of queries per domain to fetch, '
                        'default %(default)s')

    parser.add_argument('-B', '--query_blacklist', type=str,
                        help='File with list of queries to exclude')

    parser.add_argument('--domain', dest='domains', action='append',
                        help='List of domains to sample from')

    parser.add_argument('--query_filter', dest='query_filters', action='append',
                        help='A simple filter function to include for queries')

    args = parser.parse_args()

    db_conn = psycopg2.connect(args.db_conn_str)

    query_filters = args.query_filters or []
    query_filters = [eval(filter_str) for filter_str in query_filters]

    domain_queries = get_domain_query_sample_from_logs(
        db_conn, args.query_logs_json, args.num_domains, args.queries_per_domain,
        query_blacklist_file=args.query_blacklist, query_filters=query_filters)

    print "domain\tquery\tcount"

    for (domain, query, count) in domain_queries:
        print "{}\t{}\t{}".format(domain, query.encode("utf-8"), count)
