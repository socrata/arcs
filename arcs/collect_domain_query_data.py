import codecs
import argparse
import logging
import re
import simplejson
import sys
import pandas as pd
from collection import sample_queries_by_domain, apply_filters

_HAS_LETTERS_RE = re.compile("[A-Za-z]")


def query_filter(q):
    return bool(_HAS_LETTERS_RE.search(q))

if __name__ == "__main__":
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)

    parser = argparse.ArgumentParser(
        description='Gather top domains and queries from parsed nginx logs')

    parser.add_argument('query_logs_json',
                        help='JSON file containing the output of logparse.py')

    parser.add_argument('-d', '--num_domains', dest='num_domains', type=int,
                        default=10,
                        help='Number of domains to fetch queries and results '
                        'for, default %(default)s')

    parser.add_argument('-q', '--num_queries', dest='queries_per_domain',
                        type=int, default=5,
                        help='Number of queries per domain to fetch, '
                        'default %(default)s')

    args = parser.parse_args()

    logging.info("Reading query logs from {}".format(args.query_logs_json))

    with open(args.query_logs_json, "r") as f:
        df = pd.DataFrame(
            (x for x in (simplejson.loads(line.strip()) for line in f)
             if apply_filters(x)))

    logging.info("Filtering queries")

    df = df[df["query"].apply(query_filter)]

    logging.info("Sampling queries by domain")

    domain_queries = sample_queries_by_domain(
        df, args.num_domains, args.queries_per_domain,
        domain_buffer_factor=None, query_buffer_factor=None)

    for (domain, query, count) in domain_queries:
        print "{}\t{}\t{}".format(domain, query, count)
