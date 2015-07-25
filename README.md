Assessing Relevance of Catalog Search (ARCS)
============================================

ARCS is a library for Assessing Relevance of our Catalog Search system.
Specifically, it is intended to help us collect of relevance judgments from
crowdsourcing workers, and from those judgments, to compute relevance metrics
such as normalized discounted cumulative gain (NDCG) and mean average precision
(MAP).

## Creating a new CrowdFlower job

The CrowdFlower UI is pretty self-explanator. Once we settle on a task design,
creating new jobs can be done from the UI by clicking on an existing job, and
electing to copy that job with *gold units* only. As a general rule, the number
of gold units should probably be greater than or equal to 10% of the total
number of rows in a job.

### Collecting data

### Uploading data

## Measuring relevance

## References

[NDCG](https://en.wikipedia.org/wiki/Discounted_cumulative_gain)

[MAP](https://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision)

["Measuring Search Relevance", Hugh Williams](http://hughewilliams.com/2014/10/11/measuring-search-relevance/)
