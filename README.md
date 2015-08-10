Assessing Relevance of Catalog Search (ARCS)
============================================

ARCS is a library for Assessing Relevance of our Catalog Search system.
Specifically, it is intended to help us collect of relevance judgments from
crowdsourcing workers, and from those judgments, to compute relevance metrics
such as normalized discounted cumulative gain (NDCG) and mean average precision
(MAP).

## Creating a new CrowdFlower job

### Collecting data
In order to run collection.py, you must set the `METADB_CONN_STR` environment variable, and put the username
and password in your .pgpass file (you can find those in Lastpass if you search for `metadb`):

```
export METADB_CONN_STR=postgresql://animl:animl@metadba.sea1.socrata.com:5432/blist_prod
```

### Uploading data

## Measuring relevance

## References

[NDCG](https://en.wikipedia.org/wiki/Discounted_cumulative_gain)

[MAP](https://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision)

["Measuring Search Relevance", Hugh Williams](http://hughewilliams.com/2014/10/11/measuring-search-relevance/)
