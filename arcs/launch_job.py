import argparse
import os
import pandas as pd
import requests
import logging
import re
import psycopg2
from spacy.en import English, LOCAL_DATA_DIR
from frozendict import frozendict
from datetime import datetime
from experiment import GroupDefinition
from db import find_judged_qrps, insert_incomplete_job, add_raw_group_results
from db import insert_unjudged_data_for_group, insert_empty_group
from crowdflower import create_job_from_copy, add_data_to_job
from collect_domain_query_data import lang_filter

data_dir = os.environ.get('SPACY_DATA', LOCAL_DATA_DIR)
nlp = English(data_dir=data_dir)

CORE_COLUMNS = ['domain', 'query', 'result_fxf', 'result_position', 'group_id', '_golden']
DISPLAY_DATA = ['domain_logo_url', 'name', 'link', 'description']
CSV_COLUMNS = CORE_COLUMNS + DISPLAY_DATA
RAW_COLUMNS = ['domain', 'query', 'results', 'group_id']


def get_cetera_results(domain_query_pairs, cetera_host, cetera_port,
                       num_results=10, cetera_params=None,
                       filter_too_few_results=False):
    """
    Get the top n=num_results catalog search results from Cetera for each
    (domain, query) pair in domain_query_pairs.

    Args:
        domain_query_pairs (iterable): An iterable of domain-query pairs
        cetera_host (str): The Cetera hostname
        cetera_port (int): The port on which to make requests to Cetera
        num_results (int): The number of results to fetch for each query
        cetera_params (dict): Optional, additional query parameters for Cetera
        filter_too_few_results (bool): Whether to filter queries with too few results

    Returns:
        A list of (domain, query, result dict) triples
    """
    logging.info("Getting search results from Cetera")

    # we can't use the port in this version
    if 'https://api.us.socrata.com/api/catalog' in cetera_host:
        url = cetera_host
    else:
        url = "http://{}:{}/catalog".format(cetera_host, cetera_port)

    cetera_params = cetera_params or {}
    cetera_params.update({"limit": num_results * 2})  # 2x because we're going to langfilter

    params = frozendict(cetera_params)

    def _get_result_list(domain, query):
        r = requests.get(url, params=params.copy(domains=domain, q=query))
        return [res for res in list(enumerate(r.json().get("results")))
                if lang_filter(res[1]['resource'].get('description'))][:num_results]

    res = [(d, q, _get_result_list(d, q)) for d, q in domain_query_pairs]

    # filter for only the (d, q, result_list) tuples that have at least
    # num_results results
    if filter_too_few_results:
        filtered = [(d, q, rl) for d, q, rl in res if len(rl) >= num_results]
        res = filtered

    return res


def _join_sentences(acc, sentences, max_length):
    if len(sentences) > 0 and len(acc + " " + sentences[0]) < max_length:
        return _join_sentences(acc + " " + sentences.pop(0), sentences, max_length)
    else:
        return acc.strip()


def cleanup_description(desc):
    """
    Make a dataset description is readable and not ridiculously long.

    Args:
        desc (str): A dataset (or other core object) description

    Returns:
        Returns a trimmed version of the description, containing as many sentences from the
        description as can fit in a string without exceeding 400 characters
    """
    desc = desc.replace("\r", "\n")
    desc_doc = nlp(desc) if desc else desc
    desc_sentences = [s.text.replace("\n", " ").strip() for s in desc_doc.sents] if desc else []

    return _join_sentences("", desc_sentences, 400) if desc else desc


def _transform_cetera_result(result, result_position, logos):
    """
    Utility function for transforming Cetera result dictionary into something
    more suitable for the crowdsourcing task. Presently, we're grabbing name,
    link (ie. URL), and the first sentence of description.
    """
    desc = cleanup_description(result["resource"].get("description"))
    domain = result["metadata"]["domain"]

    return {"result_position": result_position,
            "result_fxf": result["resource"].get("id"),
            "name": result["resource"].get("name"),
            "link": result["link"],
            "description": desc,
            "domain_logo_url": logos.get(domain),
            "_golden": False}  # we need this field to copy data from existing CrowdFlower job


def raw_results_to_dataframe(group_results, group_id, logos):
    """
    Add group ID to raw results tuple.

    We keep raw results around for posterity

    Args:
        group_results (iterable): An iterable of results tuples as returned by get_cetera_results
        group_id (int): An identifier for the group of results

    Returns:
        An iterable of result dictionaries with the required and relevant metadata
    """
    results = pd.DataFrame.from_records(
        [(results + (group_id,)) for results in group_results],
        columns=RAW_COLUMNS)

    results["results"] = results["results"].apply(
        lambda rs: [_transform_cetera_result(r[1], r[0], logos) for r in rs])

    return results


_LOGO_UID_RE = re.compile(r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$")


def get_domain_image(domain):
    """
    Get the site logo for the specified domain.

    Args:
        domain (str): The domain cname

    Returns:
        A URL to the domain's logo
    """
    url = 'http://{0}/api/configurations.json'.format(domain)

    params = {'type': 'site_theme',
              'defaultOnly': True,
              'merge': True}

    response = None

    try:
        response = requests.get(url, params=params, timeout=(5, 10))
        response.encoding = 'utf-8'
        response.raise_for_status()

        data = next((x for x in response.json()[0]["properties"]
                     if "name" in x and x["name"] == "theme_v2b"))

        url = data.get("value", {}).get("images", {}).get("logo_header", {}) \
                                                     .get("href")

        if url and _LOGO_UID_RE.match(url):
            url = "/api/assets/{0}".format(url)

        if not (url.startswith("http") or url.startswith("https")):
            url = "http://{0}{1}".format(domain, url)
        return url

    except IndexError as e:
        print "Unexpected result shape: zero elements in response JSON"
        print "Response: {}".format(response.content if response else None)
        print "Exception: {}".format(e.message)
    except StopIteration as e:
        print "Unable to find image properties in response JSON"
        print "Response: {}".format(response.content if response else None)
        print "Exception: {}".format(e.message)
    except Exception as e:
        print "Failed to fetch configuration for %s" % domain
        print "Response: %s" % response.content if response else None
        print "Exception: %s" % e.message


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


def expanded_results_dataframe(raw_results, logos):
    """
    Stack raw results column and join with `raw_results` dataframe such that we have one
    query-result pair per row, add in domain-logos, and write to CSV.
    """
    # create new series by stacking/expanding results list
    results_s = raw_results["results"].apply(lambda rs: pd.Series(rs))

    # drop unnecessary index, reset index to jybe w/ raw_results_df, and create new dataframe
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

    return expanded_results_df


def collect_search_results(groups, query_domain_file, num_results,
                           output_file=None, cetera_host=None, cetera_port=None):
    """
    Send queries included in `query_domain_file` to Cetera, collecting n=num_results results
    for each query. For each domain, gather URLs to domain logos. Finally, bundle everything
    up into a Pandas DataFrame. Write out full expanded results to a CSV.

    Args:
        groups (iterable): An iterable of GroupDefinitions
        query_domain_file (str): A 2-column tab-delimited file containing query-domain pairs
        num_results (int): The number of search results to fetch for each query
        output_file (str): An optional file path to which the job CSV is to be written
        cetera_host (str): An optional Cetera hostname
        cetera_port (int): An optional Cetera port number

    Returns:
        A pair containing the raw results dataframe (one row per query-domain pair) and an expanded
        results dataframe where each row corresponds to a query-result pair.
    """
    assert(num_results > 0)

    logging.info("Reading query domain pairs from {}".format(query_domain_file))

    with open(query_domain_file, "r") as f:
        next(f)
        domain_queries = [x.strip().split('\t')[:2] for x in f if x.strip()]

    logging.info("Fetching domain logos")

    domains = {x[0] for x in domain_queries}
    logos = {domain: get_domain_image(domain) for domain in domains}

    raw_results_df = pd.DataFrame(columns=RAW_COLUMNS)

    # get search results for queries in each group and combine
    for group in groups:
        results = get_cetera_results(domain_queries, cetera_host, cetera_port,
                                     num_results=num_results, cetera_params=group.params)

        raw_results_df = pd.concat(
            [raw_results_df, raw_results_to_dataframe(results, group.id, logos)])

    output_file = output_file or \
        "{}-full.csv".format(datetime.now().strftime("%Y%m%d"))

    expanded_results_df = expanded_results_dataframe(raw_results_df, logos)[CSV_COLUMNS]
    expanded_results_df.to_csv(output_file, encoding="utf-8")

    return raw_results_df, expanded_results_df


def submit_job(db_conn, groups, data_df, output_file=None, job_to_copy=None):
    """
    Create CrowdFlower job for catalog search result data in `data_df`.

    An external CrowdFlower ID is created by launching an initial empty job (using a previous job
    (including settings and test data) as the initial state. After creating an CrowdFlower job and
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
    logging.info("Creating CrowdFlower job")

    # create empty CrowdFlower job by copying test units from existing job
    job = create_job_from_copy(job_id=job_to_copy)

    # filter previously judged QRPs, so that we don't pay to have them rejudged
    num_rows_pre_filter = len(data_df)
    data_df = filter_previously_judged(db_conn, data_df)
    num_rows_post_filter = len(data_df)

    logging.info("Eliminated {} rows that had been previously judged".format(
        num_rows_pre_filter - num_rows_post_filter))

    # multiple groups may in fact produce the same results, for any given query,
    # so let's ensure we're having each (query, result) pair judged only once
    grouped = data_df.groupby(["query", "result_fxf"])
    data_df = grouped.first().reset_index()

    logging.info("Eliminated {} redundant query-result rows".format(
        num_rows_post_filter - len(data_df)))

    output_file = output_file or \
        "{}-crowdflower.csv".format(datetime.now().strftime("%Y%m%d"))

    logging.info("Writing out {} rows as CSV to {}".format(len(data_df), output_file))

    data_df.to_csv(output_file, encoding="utf-8",
                   index=False, escapechar="\\", na_rep=None)

    logging.info("Adding data to job from CSV")

    try:
        add_data_to_job(job.external_id, output_file)
    except Exception as e:
        logging.warn("Unable to send CSV to CrowdFlower: {}".format(e.message))
        logging.warn("Try uploading the data manually using the web UI.")

    logging.info("Job submitted.")
    logging.info("Job consists of {} group(s): {}".format(
        len(groups), '\n'.join([str(g) for g in groups])))

    logging.info("https://make.crowdflower.com/jobs/{}".format(job.external_id))

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
    logging.info("Writing incomplete job to DB")

    job = insert_incomplete_job(db_conn, job)

    # we store display data as JSON in the DB, so let's add a new column of just that
    raw_data_df["payload"] = pd.Series(_df_data_to_records(raw_data_df[RAW_COLUMNS]))

    # write all QRPs to DB
    logging.info("Writing query-result pairs to DB")

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


def arg_parser():
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
    """
    - read domain query pairs from file specified at the command-line
    - create and persist groups defined at the command-line
    - collect results for queries in each group
    - combine data from each group
    - filter out previously judged QRPs
    - write all data to CSV
    - create CrowdFlower job from CSV
    - persist all data to DB for posterity
    """
    args = arg_parser()

    db_conn = psycopg2.connect(args.db_conn_str)

    groups = args.groups or [GroupDefinition("baseline", "", {})]
    groups = [insert_empty_group(db_conn, group) for group in groups]

    raw_results_df, expanded_results_df = collect_search_results(
        db_conn, groups, args.input_file, args.num_results,
        args.full_csv_file, args.cetera_host, args.cetera_port)

    job = submit_job(
        db_conn, groups, expanded_results_df, args.crowdflower_csv_file, args.job_to_copy)

    persist_job_data(db_conn, job, groups, raw_results_df)

    db_conn.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
