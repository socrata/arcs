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
export METADB_CONN_STR=postgresql://$USERNAME:$USERNAME@$METADB_HOSTNAME:$METADB_PORT/$METADB_TABLENAME
```

Replacing `$USERNAME`, `$METADB_HOSTNAME`, `$METADB_PORT` and `$METADB_TABLENAME` appropriately.

### Uploading data

## Measuring relevance

Once a job has completed, you can download the results and report NDCG by
running the `download_crowdflower.sh` script. Before doing so, ensure that you
have a a CrowdFlower API key (which you can find by logging into CrowdFlower and
going to https://make.crowdflower.com/account/user) and that a corresponding environment variable is
set. The `123456` in the snippet below is a specific job ID. Replace this with
the ID of the recently completed job.

```bash
export CROWDFLOWER_API_KEY=LbcxvIlE3x1M8F6TT5hN
python arcs/download_crowdflower.py -n -i 123456
```

This will report per-domain NDCG as well as overall NDCG.

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
