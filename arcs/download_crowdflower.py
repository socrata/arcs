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
        judgement = body['relevance'].get('avg')
        extracted[domain][query].append((result_position, response, judgement))

    return extracted


def main(job_idx):
    results = get_results(job_idx)
    #results = json.load(open('{}.json'.format(job_idx)))
    extracted_results = extract(results)
    all_ndcgs = []
    for dom, queries in extracted_results.iteritems():
        print dom
        dom_ndcgs = []
        for query, results in queries.iteritems():
            results.sort()
            # just throw away the "data missing/error" results
            judgements = [r[-1] for r in results if r[-1] > -1]
            if len(judgements) > 1 and sum(judgements) > 0:
                print "\t", query
                print "\t", judgements
                ndcg = compute_ndcg(judgements)
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
    all_ndcgs = [n for n in all_ndcgs if n < 1.0]
    all_ndcg = sum(all_ndcgs)/len(all_ndcgs)
    print "OVERALL NDCG:", all_ndcg


if __name__ == "__main__":
    job_idx = sys.argv[1]
    main(job_idx)
