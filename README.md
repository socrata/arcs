Assessing Relevance of Catalog Search (ARCS)
============================================

ARCS is a library for Assessing Relevance of our Catalog Search system.
Specifically, it is intended to help us collect relevance judgments from
crowdsourcing workers, and from those judgments, to compute relevance metrics
such as normalized discounted cumulative gain (NDCG) and mean average precision
(MAP).

## Installing

First, create a new virtual environment for Arcs, activate it, and then:

```bash
pip install -e .
```

## Running tests

From the Arcs virtual environment created above, do the following:

```bash
python setup.py test
```

## Parsing server logs for query data

Use the following command to parse server logs for Catalog queries and to output
them as JSON. Run this from the arcs directory. Set the dirname variable to the
path to a directory of gzipped server logs.

```sh
dirname=~/Data/query_analysis/2015-08-10.logs
for x in `ls $dirname`; do
    gzcat $dirname/$x | python arcs/logparser.py
done > 2015-08-10.logs.concat.json
```

## Collecting query data for fun and profit

You may be interested in sampling domains and queries, simply for the purpose of
eyballing results, error analysis, or for serving as the basis for a new
CrowdFlower catalog relevance task. You'll need a parsed query log JSON file
(like the one generated in the previous step). Then you can do the following:

```sh
python arcs/collect_domain_query_data.py ~/Data/query_analysis/2015-08-10.logs.concat.json
```

This will write 3-column tab-delimited lines containing (domain, query, count)
triples to STDOUT.

Not surprisingly, there is noise in the query logs. To ensure that we don't send
garbage queries to crowdsourcing workers for annotation, the
`collect_domain_query_data` script uses both a hand-curated blacklist and
filtering regex patterns to eliminate garbage queries. You may find you want to
add additional patterns or blacklist elements, which you can do easily
enough. The query blacklist is in the `data` directory. Additionally, it may be
useful to supply custom filters for particular tasks. For example, if you want
to launch a CrowdFlower task to collect judgments limited to only multi-term
queries, you can supply custom filters like so:

```sh
python arcs/collect_domain_query_data.py ~/Data/query_analysis/20150924.logs.concat.json -D 'postgresql://username:@hostname:5432/db_name' -d 10 -q 5 -B data/query_blacklist.txt --query_filter='lambda s: " " in s.strip()' > ~/Data/arcs/20151006.slop/queries.tsv
```

Here we specify an additional filter which will restrict our queries to those
containing a whitespace character that is non-initial and non-terminal.

## Creating a new CrowdFlower job

The CrowdFlower UI is pretty self-explanatory. Creating new jobs can be done
from the UI by clicking on an existing job, and electing to copy that job with
*gold units* only. As a general rule, the number of gold units should probably
be greater than or equal to 10% of the total number of rows in a
job. Additionally, it's a good idea to add to this set regularly to ensure that
workers are not being exposed to the same questions over and over again.

Any programmatic interaction with the CrowdFlower API requires that a
CrowdFlower API token be present in your shell environment. You can obtain such
a token by logging into CrowdFlower and going
[here](https://make.crowdflower.com/account/user). Set the CrowdFlower
environment variable like so:

```bash
export CROWDFLOWER_API_KEY=123456789abcdefghijk       
```

Add this to your environment resource or profile file to ensure that it is set
on login.

Note that the token included above is fake.

To simplify job creation and data bookeeping, we've added a script (launch_job),
which that will do the following:

1. collect results for all queries from an input query file from Cetera
2. store raw results data as a CSV for posterity / inspection
3. extract relevant result fields from each query-result pair to create
   CrowdFlower task
4. launch CrowdFlower task copying existing test units from existing job
5. persist job data in a postgres DB

The script can be run like so:

```sh
python arcs/launch_job.py -i ~/Data/arcs/20151006.slop/queries.tsv -g '{"name": "baseline", "description": "Current production system as of 10/6/2015", "params": {}}' -g '{"name": "Enabling slop=5", "description": "Testing the effect of slop=5 on multi-term queries", "params": {"slop": 5}}' -r 10 -c localhost -p 5704 -D 'postgresql://username:@hostname:5432/db_name' -F ~/Data/arcs/20151006.slop/full.csv -C ~/Data/arcs/20151006.slop/crowdflower.csv
```

We specify the required input file of queries w/ the `-i` flag, the parameters
of each Group of results with the `-g` flag, the number of the results with the
`-r` flag, the Cetera host and port with the `-c` and `-p` flags, our database
connection string w/ the `-D` flag, and finally, an optional path to where the
full and CrowdFlower CSVs should be written. If no groups are specified, the
default behavior is to create a group named "baseline" with an empty parameters
dict (which is used for each query to Cetera).

You may optionally specify a `--job_to_copy` (`-j`) parameter. This indicates
the CrowdFlower job that should be used as the basis for the task.

Once a job has been completed -- and you should receive an email notification to
this effect from CrowdFlower -- you can download the judgment data like so:

```sh
python arcs/fetch_job_results.py -j 786401 -D 'postgresql://username:@hostname:5432/db_name'
```

The external (CrowdFlower) job ID must be specified (`-j`/`--job_id`). As with
the launch script above, a DB connection string must be supplied
(`-D`/`--db_conn_str`).

## Measuring relevance

Once a job has completed and you've downloaded the data, you can download the
results and report various statistics (including our core relevance metric,
NDCG) by running the `summarize_results` script.

```sh
python arcs/summarize_results.py 14 27 -D 'postgresql://username:@hostname:5432/db_name'
```

This will report per-domain NDCG as well as overall NDCG. The output should look
something like this:

```json
{
    "num_unique_qrps": 569,
    "num_total_diffs": 480,
    "Including both min_should_match and title boosting": {
        "avg_ndcg_at_5": 0.6562898326463115,
        "num_zero_result_queries": 97,
        "num_queries": 139,
        "num_irrelevant": 188,
        "avg_ndcg_at_10": 0.6689914334597877,
        "precision": 0.7545454545454545,
        "unjudged_qrps": 1,
        "ndcg_error": 0.34371016735368853
    },
    "ndcg_delta": 0.03646560858737047,
    "basline": {
        "avg_ndcg_at_5": 0.619824224058941,
        "num_zero_result_queries": 63,
        "num_queries": 173,
        "num_irrelevant": 451,
        "avg_ndcg_at_10": 0.6506475987305224,
        "precision": 0.6359967715899919,
        "unjudged_qrps": 0,
        "ndcg_error": 0.380175775941059
    }
}
```

## Calculating inter-annotator agreement

It's useful to know how much agreement there is between our workers as it gives
us some signal about the difficulty, interpretability, and subjectivity of our
task. You can calculate inter-annotator agreement by first downloading non-aggregated data
from CrowdFlower (Results > Settings > "All answers" in the dropdown before downloading
the aggregated result) like so:

```bash
python arcs/calc_iaa.py -c file_from_crowdflower.csv --top_n
```

This will report
[Krippendorf's Alpha](https://en.wikipedia.org/wiki/Krippendorff%27s_alpha),
which is a statistical measure of agreement among an arbitrary number of
workers.

## Error analysis

After getting judged data back from CrowdFlower, it's a good idea to inspect the
the rows where the results were obviously bad, or where something went wrong and
prevented the workers from assigning a judgment score. You can achieve this with
the following:

```bash
python arcs/error_analysis.py 755163 -o 20150806.errors.csv
```

This will save a CSV to the path specified by the -o parameter. The rows will be
sorted by the number of bad judgments.

## References

[NDCG](https://en.wikipedia.org/wiki/Discounted_cumulative_gain)

[MAP](https://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision)

["Measuring Search Relevance", Hugh Williams](http://hughewilliams.com/2014/10/11/measuring-search-relevance/)

[Krippendorf's Alpha](https://en.wikipedia.org/wiki/Krippendorff%27s_alpha)
