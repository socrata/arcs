"""
Create a CrowdFlower job to collect relevance judments for domain, query pairs.

This script will read a file of domain, query pairs from the command-line, and collect results from
Cetera for each pair. You may optionally specify different experimental groups (eg. baseline
vs. experiment1) via the `-g` option. These should be specified as JSON strings.

Example:

    python arcs/launch_job.py \
        -i ~/Data/arcs/20160126.experiment_1/queries.tsv \
        -g '{"name": "adjusted boost clause", "description": "Moved field boosts to 'should' clause", "params": {}}' \
        -r 10 \
        -D 'postgresql://username:password@hostname/dbname'

The group definition should have a name, description, and params field. The params field should be
a nested object specifying any relevant parameters of the experiment.

A full CSV is created, which contains all of the job data. Additionally, a CrowdFlower CSV is
created which corresponds precisely with the data uploaded to create the job in CrowdFlower.

All data is persisted in a Postgres database, the parameters of which are specified via the -D
option.
"""
import argparse
import pandas as pd
import logging
import psycopg2
from functools import partial
from datetime import datetime

from cetera import get_cetera_results
from crowdflower import create_job_from_copy, add_data_to_job
from crowdsourcing_utils import cleanup_description, make_dataset_sample
from db import (
    find_judged_qrps, insert_incomplete_job, add_raw_group_results,
    insert_unjudged_data_for_group, insert_empty_group
)
from experiment import GroupDefinition


CORE_COLUMNS = ['domain', 'query', 'result_fxf', 'result_position', 'group_id']
DISPLAY_DATA = ['name', 'link', 'description', 'sample']
CSV_COLUMNS = CORE_COLUMNS + DISPLAY_DATA
RAW_COLUMNS = ['domain', 'query', 'results', 'group_id']

logging.basicConfig(format='%(message)s', level=logging.INFO)
LOGGER = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


def _transform_cetera_result(result, result_position, num_rows, num_columns):
    """
    Utility function for transforming Cetera result dictionary into something
    more suitable for the crowdsourcing task. Presently, we're grabbing name,
    link (ie. URL), and the first sentence of description.

    Args:
        result ():
        result_position (int): The position of the result in the result set
        num_rows (int): The number of rows to show in the dataset sample
        num_columns (int): The number of columns to show in the dataset sample

    Returns:
        A dictionary of data for each result
    """
    desc = cleanup_description(result["resource"].get("description"))
    domain = result["metadata"]["domain"]
    fxf = result["resource"].get("id")
    data_sample = make_dataset_sample(domain, fxf, num_rows, num_columns)

    return {
        "result_domain": domain,
        "result_position": result_position,
        "result_fxf": fxf,
        "name": result["resource"].get("name"),
        "link": result["link"],
        "description": desc,
        "sample": data_sample
    }


def raw_results_to_dataframe(group_results, group_id, num_rows, num_columns):
    """
    Add group ID to raw results tuple.

    Notes:
        1. We keep raw results around for posterity.
        2. When domain is specified as "www.opendatanetwork.com" in the input, we replace it with
           the source domain of the corresponding result

    Args:
        group_results (iterable): An iterable of results tuples as returned by get_cetera_results
        group_id (int): An identifier for the group of results
        num_rows (int): The number of rows to show in the dataset sample
        num_columns (int): The number of columns to show in the dataset sample

    Returns:
        An iterable of result dictionaries with the required and relevant metadata
    """
    LOGGER.info("Transforming raw results")

    results = pd.DataFrame.from_records(
        [(results + (group_id,)) for results in group_results],
        columns=RAW_COLUMNS)

    transform = partial(_transform_cetera_result, num_rows=num_rows, num_columns=num_columns)

    results["results"] = results["results"].apply(lambda rs: [transform(r[1], r[0]) for r in rs])
    results["query"] = results["query"].apply(str)

    return results


def filter_previously_judged(db_conn, qrps_df):
    """
    Filter a Pandas DataFrame of query-result pairs to only those that have not
    previously been judged.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        qrps_df (pandas.DataFrame): A DataFrame of query, result data

    Returns:
        A copy of the input DataFrame filtered down to unjudged QRPs
    """
    previously_judged = find_judged_qrps(db_conn)

    return qrps_df[qrps_df.apply(
        lambda row: (row["query"], row["result_fxf"]) not in previously_judged, axis=1)]


def expanded_results_dataframe(raw_results):
    """
    Stack raw results column and join with `raw_results` dataframe such that we have one
    query-result pair per row.

    Args:
        raw_results (pandas.DataFrame): A DataFrame with queries and results

    Returns:
        An expanded DataFrame with on query-result pair per row
    """
    # create new series by stacking/expanding results list
    results_s = raw_results["results"].apply(lambda rs: pd.Series(rs))

    # drop unnecessary index, reset index to jibe w/ raw_results_df, and create new dataframe
    expanded_results_df = pd.DataFrame(
        {"result": results_s.unstack().reset_index(level=0, drop=True)})

    # join w/ original dataframe
    expanded_results_df = raw_results.join(expanded_results_df)

    # filter all rows for which there are zero results
    expanded_results_df = expanded_results_df[expanded_results_df["result"].notnull()]\
        .reset_index()

    # add columns from fields in dict
    results_dict_df = pd.DataFrame.from_records(list(expanded_results_df["result"]))
    results_dict_df.set_index(expanded_results_df.index, inplace=True)
    expanded_results_df = expanded_results_df.join(results_dict_df)

    # drop original domain, and replace with result domain
    expanded_results_df = expanded_results_df.drop("domain", 1)
    expanded_results_df = expanded_results_df.rename(columns={"result_domain": "domain"})

    return expanded_results_df


def collect_search_results(groups, query_domain_file, num_results, num_rows, num_columns,
                           output_file=None, cetera_host=None, cetera_port=None):
    """
    Send queries included in `query_domain_file` to Cetera, collecting n=num_results results
    for each query. Bundle everything up into a Pandas DataFrame. Write out full expanded results
    to a CSV.

    Args:
        groups (Iterable[GroupDefinition]): An iterable of GroupDefinitions
        query_domain_file (str): A 2-column tab-delimited file containing query-domain pairs
        num_results (int): The number of search results to fetch for each query
        num_rows (int): The number of rows to show in the dataset sample
        num_columns (int): The number of columns to show in the dataset sample
        output_file (str): An optional file path to which the job CSV is to be written
        cetera_host (str): An optional Cetera hostname
        cetera_port (int): An optional Cetera port number

    Returns:
        A pair containing the raw results dataframe (one row per query-domain pair) and an expanded
        results dataframe where each row corresponds to a query-result pair.
    """
    assert(num_results > 0)

    LOGGER.info("Reading query domain pairs from {}".format(query_domain_file))

    with open(query_domain_file, "r") as f:
        next(f)  # skip header
        domain_queries = [tuple(x.strip().split('\t')[:2]) for x in f if x.strip()]

    raw_results_df = pd.DataFrame(columns=RAW_COLUMNS)

    # get search results for queries in each group and combine
    for group in groups:
        results = get_cetera_results(domain_queries, cetera_host, cetera_port,
                                     num_results=num_results, cetera_params=group.params)

        raw_results_df = pd.concat(
            [raw_results_df, raw_results_to_dataframe(results, group.id, num_rows, num_columns)])

    output_file = output_file or \
        "{}-full.csv".format(datetime.now().strftime("%Y%m%d"))

    expanded_results_df = expanded_results_dataframe(raw_results_df)[CSV_COLUMNS]
    expanded_results_df.to_csv(output_file, encoding="utf-8")

    return raw_results_df, expanded_results_df


def submit_job(db_conn, groups, data_df, output_file=None, job_to_copy=None):
    """
    Create CrowdFlower job for catalog search result data in `data_df`.

    An external CrowdFlower ID is created by launching an initial empty job (using a previous job
    (including settings and test data) as the initial state. After creating a CrowdFlower job and
    getting an external ID, we persist the job itself to the DB

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        groups (iterable): An iterable of GroupDefinitions
        data_df (pandas.DataFrame): A DataFrame of query, result data
        output_file (str): Optional path to a CSV file to be created and submitted to CrowdFlower
        job_to_copy (int): Optional external identifier for existing job to copy for its test data

    Returns:
        An Arcs Job with its external ID populated
    """
    LOGGER.info("Creating CrowdFlower job")

    # create empty CrowdFlower job by copying test units from existing job
    job = create_job_from_copy(job_id=job_to_copy)

    # filter previously judged QRPs, so that we don't pay to have them rejudged
    num_rows_pre_filter = len(data_df)
    data_df = filter_previously_judged(db_conn, data_df)
    num_rows_post_filter = len(data_df)

    LOGGER.info("Eliminated {} rows that had been previously judged".format(
        num_rows_pre_filter - num_rows_post_filter))

    # multiple groups may in fact produce the same results, for any given query,
    # so let's ensure we're having each (query, result) pair judged only once
    grouped = data_df.groupby(["query", "result_fxf"])
    data_df = grouped.first().reset_index()

    LOGGER.info("Eliminated {} redundant query-result rows".format(
        num_rows_post_filter - len(data_df)))

    output_file = output_file or \
        "{}-crowdflower.csv".format(datetime.now().strftime("%Y%m%d"))

    LOGGER.info("Writing out {} rows as CSV to {}".format(len(data_df), output_file))

    data_df.to_csv(output_file, encoding="utf-8",
                   index=False, escapechar="\\", na_rep=None)

    LOGGER.info("Adding data to job from CSV")

    try:
        add_data_to_job(job.external_id, output_file)
    except Exception as e:
        LOGGER.warn("Unable to send CSV to CrowdFlower: {}".format(e.message))
        LOGGER.warn("Try uploading the data manually using the web UI.")

    LOGGER.info("Job submitted.")
    LOGGER.info("Job consists of {} group(s): {}".format(
        len(groups), '\n'.join([str(g) for g in groups])))

    LOGGER.info("https://make.crowdflower.com/jobs/{}".format(job.external_id))

    return job


def _df_data_to_records(df):
    return (dict(zip(df.columns, record)) for record in df.to_records(index=False))


def persist_job_data(db_conn, job, groups, raw_data_df):
    """
    Write all job data to the DB.

    We write an initial incomplete job, using the external ID populated upon job submission. We
    store the job unit data in a JSON blob in the DB. And we write group-specific data to the DB
    without any judgments that will be updated upon job completion. The input data should be the
    full DataFrame, as opposed to the deduplicated data we send to CrowdFlower.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        job (Job): An Arcs Job object with, at a minimum, its external_id set
        groups (iterable): An iterable of GroupDefinitions
        data_df (pandas.DataFrame): A DataFrame of query, result data
        raw_data_df (pandas.DataFrame): A DataFrame of raw results where each row corresponds to a
            query, and results are left in a collection

    Returns:
        None
    """
    LOGGER.info("Writing incomplete job to DB")

    job = insert_incomplete_job(db_conn, job)

    # we store display data as JSON in the DB, so let's add a new column of just that
    raw_data_df["payload"] = pd.Series(_df_data_to_records(raw_data_df[RAW_COLUMNS]))

    # write all QRPs to DB
    LOGGER.info("Writing query-result pairs to DB")

    for group in groups:
        # filter to just this group
        raw_group_data = raw_data_df[raw_data_df["group_id"] == group.id].drop(
            "group_id", axis=1, inplace=False)

        # persist the raw group data for posterity
        add_raw_group_results(db_conn, group.id, list(raw_group_data["payload"]))

        # insert all query/result data
        insert_unjudged_data_for_group(db_conn, job.id, group.id,
                                       _df_data_to_records(raw_group_data))

    # drop the payload column we added above
    raw_data_df.drop("payload", axis=1, inplace=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Gather domains and queries from parsed nginx logs, '
        'gather the top n results from cetera')

    parser.add_argument('-i', '--input_file', required=True,
                        help='Tab-delimited file of queries and domains to as the basis for \
                        the crowdsourcing task')

    parser.add_argument('-D', '--db_conn_str', required=True,
                        help='Database connection string')

    parser.add_argument('-F', '--full_csv_file',
                        help='Path for full CSV file with full set of search result data')

    parser.add_argument('-C', '--crowdflower_csv_file',
                        help='Path for filtered CSV file restricted to set query-result pairs \
                        requiring judgment')

    parser.add_argument('-r', '--num_results', dest='num_results', type=int,
                        default=40,
                        help='Number of results per (domain, query) pair to fetch from cetera, \
                        default %(default)s')

    parser.add_argument('-c', '--cetera_host', dest='cetera_host',
                        default='https://api.us.socrata.com/api/catalog/v1',
                        help='Cetera hostname (eg. localhost) \
                        default %(default)s')

    parser.add_argument('-p', '--cetera_port', dest='cetera_port',
                        default='80',
                        help='Cetera port, default %(default)s')

    parser.add_argument('-g', '--group', dest='groups', type=GroupDefinition.from_json,
                        action="append")

    parser.add_argument('-j', '--job_to_copy', type=int, default=None,
                        help='CrowdFlower job ID to copy for test data units')

    return parser.parse_args()


def main():
    args = parse_args()

    db_conn = psycopg2.connect(args.db_conn_str)

    groups = args.groups or [GroupDefinition(name="baseline", description="", params={})]
    groups = [insert_empty_group(db_conn, group) for group in groups]

    raw_results_df, expanded_results_df = collect_search_results(
        groups, args.input_file, args.num_results,
        args.full_csv_file, args.cetera_host, args.cetera_port)

    job = submit_job(
        db_conn, groups, expanded_results_df, args.crowdflower_csv_file, args.job_to_copy)

    persist_job_data(db_conn, job, groups, raw_results_df)

    db_conn.commit()


if __name__ == "__main__":
    main()
