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

## Parsing Nginx logs for query data

Use the following command to parse Nginx logs for Catalog queries and to output
them as JSON. Run this from the arcs directory. TODO: make this happen
periodically and automatically. Set the dirname variable to the path to a
directory of gzipped Nginx logs (as downloaded from one of our front-end load
balancers). 

```sh
dirname=~/Data/query_analysis/2015-08-10.logs
for x in `ls $dirname`; do
    gzcat $dirname/$x | python arcs/logparser.py
done > 2015-08-10.logs.concat.json
```

## Collecting query data for fun and profit

You may be interested in sampling domains and queries, simply for the purpose of
eyballing results or error analysis. You'll need a parsed query log JSON file
(like the one generated in the previous step). Then you can do the following:

```sh
python arcs/collect_domain_query_data.py ~/Data/query_analysis/2015-08-10.logs.concat.json
```

This will write 2-column tab-delimited lines containing (domain, query) pairs to
STDOUT.

## Creating a new CrowdFlower job

The CrowdFlower UI is pretty self-explanatory. Creating new jobs can be done
from the UI by clicking on an existing job, and electing to copy that job with
*gold units* only. As a general rule, the number of gold units should probably
be greater than or equal to 10% of the total number of rows in a job.

### Collecting data

In order to run collection.py, you must set the `METADB_CONN_STR` environment
variable, and put the username and password in your .pgpass file (you can find
those in Lastpass if you search for `metadb`):

```bash
export METADB_CONN_STR=postgresql://animl:animl@metadba.sea1.socrata.com:5432/blist_prod
```

### Uploading data

## Measuring relevance

Once a job has completed, you can download the results and report NDCG by
running the `download_crowdflower.sh` script. Before doing so, ensure that you
have a a CrowdFlower API key and that a corresponding environment variable is
set. The `755163` in the snippet below is a specific job ID. Replace this with
the ID of the recently completed job.

```bash
export CROWDFLOWER_API_KEY=LbcxvIlE3x1M8F6TT5hN
python arcs/download_crowdflower.py -n -i 755163
```

This will report per-domain NDCG as well as overall NDCG.

## Calculating inter-annotator agreement

It's useful to know how much agreement there is between our workers as it gives
us some signal about the difficulty, interpretability, and subjectivity of our
task. You can calculate inter-annotator agreement like so:

```bash
python arcs/calc_iaa.py -c data/20150806/all.csv --top_n
```

This will report
[Krippendorf's Alpha](https://en.wikipedia.org/wiki/Krippendorff%27s_alpha),
which is a statistical measure of agreement among an arbitrary number of
workers.

## References

[NDCG](https://en.wikipedia.org/wiki/Discounted_cumulative_gain)

[MAP](https://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision)

["Measuring Search Relevance", Hugh Williams](http://hughewilliams.com/2014/10/11/measuring-search-relevance/)

[Krippendorf's Alpha](https://en.wikipedia.org/wiki/Krippendorff%27s_alpha)
