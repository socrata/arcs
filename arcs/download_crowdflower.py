import argparse
import os
import json
import requests
from evaluation import ndcg
from collections import defaultdict


def get_judgments(job_idx):
    """
    Download the raw judgments from CrowdFlower

    Args:
        job_ids: A unique identifier for the CrowdFlower job.

    Returns: A dictionary of raw judgment data from the CrowdFlower API, where
        each key is an ID for a unit of work (a row).
    """
    api_key = os.environ['CROWDFLOWER_API_KEY']
    url = "https://api.crowdflower.com/v1/jobs/{job_id}/judgments.json?key={api_key}&page={page}"
    response = True
    page = 1
    _all = {}
    while response:
        # sweet API, need to page through
        filled_url = url.format(job_id=job_idx, api_key=api_key, page=page)
        print filled_url
        resp = requests.get(filled_url)
        response = resp.json()
        _all.update(response)
        page += 1

    with open('{}.json'.format(job_idx), 'w') as outfile:
        json.dump(_all, outfile, indent=4, sort_keys=True)

    return _all


def extract(results):
    """extract what we want:
    domain, query, response (name, description), result_position, judgement"""
    extracted = defaultdict(lambda: defaultdict(list))
    for idx, body in results.iteritems():
        domain = body.get('domain')
        query = body.get('query')
        response = (body.get('name'), body.get('description'))
        result_position = body.get('result_position')
        # just use the avg for now
        if isinstance(body['relevance'], dict):
            judgement = body['relevance'].get('avg')
        elif isinstance(body['relevance'], list):
            judgments = [float(j) for j in body.get('relevance')]
            judgement = sum(judgments)/len(judgments)
        extracted[domain][query].append((result_position, response, judgement))

    return extracted


def print_ndcg(extracted_results):
    all_ndcgs = []

    for dom, queries in extracted_results.iteritems():
        print dom
        dom_ndcgs = []
        for query, results in queries.iteritems():
            results.sort()
            # just throw away the "data missing/error" results
            judgments = [r[-1] for r in results if r[-1] > -1]
            diff = (len(judgments) != len(results))
            if diff:
                print "removed `data missing/error` judgments:\n{}\n{}".format(
                    [r[-1] for r in results], judgments)

            if len(judgments) > 1 and sum(judgments) > 0:
                print "\t", query
                print "\t", judgments

                try:
                    _ndcg = ndcg(judgments)
                    print "\t\t", _ndcg
                    all_ndcgs.append(_ndcg)
                    dom_ndcgs.append(_ndcg)

                except ZeroDivisionError:
                    print "Error calculating NDCG for query {0} with judgments" \
                        "{1}".format(query, judgments)

        if dom_ndcgs:
            dom_ndcg = sum(dom_ndcgs)/len(dom_ndcgs)
            print "DOMAIN NDCG:", dom_ndcg

        print '='*30
        print

    # for now throw out the 1-values, we don't have enough data
    # for them to be "real" 1s
    all_ndcgs_lt_ones = [n for n in all_ndcgs if n < 1.0]

    diff = len(all_ndcgs) - len(all_ndcgs_lt_ones)
    if diff:
        print "removed {} NDCG scores of 1 (insufficient data)".format(diff)
        print

    all_ndcg = sum(all_ndcgs_lt_ones) / len(all_ndcgs_lt_ones)

    print "OVERALL NDCG:", all_ndcg
    print "OVERALL NDCG ERROR:", 1 - all_ndcg


def arg_parser():
    parser = argparse.ArgumentParser(description='Download data from CrowdFlower. Optionally report NDCG')

    parser.add_argument('-c', '--cache', dest='cache', default=False, action='store_true',
                        help='Whether to read from cache, default %(default)s')
    parser.add_argument('-n', '--ndcg', dest='ndcg', default=False, action='store_true',
                        help='Report NDCG?, default %(default)s')
    parser.add_argument('-i', '--job_id', dest='job_id', required=True,
                        help='Job ID')

    args = parser.parse_args()
    return args


def main(job_idx, cache, ndcg):
    if cache:
        results = json.load(open('{}.json'.format(job_idx)))
    else:
        results = get_judgments(job_idx)
    if ndcg:
        extracted_results = extract(results)
        print_ndcg(extracted_results)


if __name__ == "__main__":
    args = arg_parser()
    main(args.job_id, args.cache, args.ndcg)
