Assessing Relevance of Catalog Search (ARCS)
============================================

ARCS is a library for Assessing Relevance of our Catalog Search system.
Specifically, it is intended to help us collect of relevance judgments from
crowdsourcing workers, and from those judgments, to compute relevance metrics
such as normalized discounted cumulative gain (NDCG) and mean average precision
(MAP).

## Installing

First, create a new virtual environment for Arcs, activate it, and then:

```bash
pip install -e .
```

## Running tests

From a fresh arcs virtual environment, install pytest:

```bash
pip install pytest
```

And then run the tests

```bash
py.test
```

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
       
## References

[NDCG](https://en.wikipedia.org/wiki/Discounted_cumulative_gain)

[MAP](https://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision)

["Measuring Search Relevance", Hugh Williams](http://hughewilliams.com/2014/10/11/measuring-search-relevance/)
