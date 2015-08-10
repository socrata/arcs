import argparse
import sys
import os
import json
import requests
from measure_ndcg import compute_ndcg
from collections import defaultdict


def get_results(job_idx):
    """grab the results from crowdflower, optionally dump them as JSON"""
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
                print "removed `data missing/error` judgments:\n{}\n{}".format([r[-1] for r in results], judgments)
            if len(judgments) > 1 and sum(judgments) > 0:
                print "\t", query
                print "\t", judgments
                ndcg = compute_ndcg(judgments)
                if ndcg != None:
                    print "\t\t", ndcg
                    all_ndcgs.append(ndcg)
                    dom_ndcgs.append(ndcg)
        if dom_ndcgs:
            dom_ndcg = sum(dom_ndcgs)/len(dom_ndcgs)
            print "DOMAIN NDCG:", dom_ndcg
        print '='*30
        print
    # for now throw out the 1-values, we don't have enough data
    # for them to be "real" 1s
    all_ndcgs_minus_ones = [n for n in all_ndcgs if n < 1.0]
    diff = (len(all_ndcgs) != len(all_ndcgs_minus_ones))
    if diff:
        print "removed ndcgs with a score of 1:\n{}\n{}"
    all_ndcg = sum(all_ndcgs_minus_ones)/len(all_ndcgs_minus_ones)
    print "OVERALL NDCG:", all_ndcg


def arg_parser():
    parser = argparse.ArgumentParser(description='Download data from crowdflower. Optionally report NDCG')

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
        results = get_results(job_idx)
    if ndcg:
        extracted_results = extract(results)
        print_ndcg(extracted_results)


if __name__ == "__main__":
    args = arg_parser()
    main(args.job_id, args.cache, args.ndcg)
