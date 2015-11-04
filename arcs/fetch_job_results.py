import argparse
import logging
import os
import psycopg2
import psycopg2.extras
import sys
from db import update_completed_job, add_judgments_for_qrps
from crowdflower import get_job_metadata, get_job_results


HERE = os.path.dirname(os.path.abspath(__file__))
# TODO: if we ever allow more platforms, we'll have to add API
# wrappers for them, and then add them to this list
ALLOWED_CROWDSOURCE_PLATFORMS = frozenset(('crowdflower',))


def create_tables(conn, sql_file=None):
    """
    Run the table initialization SQL contained in sql_file.

    Args:
        conn (psycopg2.extensions.connection): Connection to the DB
        sql_file (str): path to a file containing valid table
            creation SQL, *hopefully of the type CREATE TABLE IF NOT
            EXISTS*
            If absent, defaults to ./sql/create_arcs_tables.sql

    Returns:
        None
    """
    cur = conn.cursor()
    sql_file = sql_file or os.path.join(HERE, 'sql', 'create_arcs_tables.sql')
    with open(sql_file) as infi:
        sql = infi.read()

    cur.execute(sql)
    conn.commit()


def get_job_data(crowdsource_platform, external_job_id, api_key=None):
    """
    Get job metadata and data from the platform.

    Args:
        crowdsource_platform (str): name of the crowdsourcing platform (eg. crowdflower)
        external_job_id (str): id of the job, according to the platform

    Returns:
        metadata (dict): metadata blob (eg. prompt, HTML...)
        job_created_at (datetime): time of job creation
        job_completed_at (datetime or None): time of job completion, if completed, else None
        data (list): list of dicts of (query, result_fxf, judgment)
        full_json (dict): the full return content from crowdflower, keyed by line number

    """
    logging.info("Gathering data and metadata from {}".format(crowdsource_platform))

    if crowdsource_platform == 'crowdflower':
        api_key = api_key or os.environ['CROWDFLOWER_API_KEY']
        metadata, job_created_at, job_completed_at = get_job_metadata(external_job_id, api_key)
        data, full_json = get_job_results(external_job_id, api_key)
    else:
        logging.error('Unexpected crowdsource_platform {}, \
        must be one of {}'.format(crowdsource_platform,
                                  ALLOWED_CROWDSOURCE_PLATFORMS))
        sys.exit(1)

    return metadata, job_created_at, job_completed_at, data, full_json


def fetch_and_write_job_results(db_conn, external_job_id):
    """
    - download and parse job data
    - write job result to arcs_job table
    - write results to arcs_query_result
    - write results to arcs_group_join

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database: A DB connection string
        external_job_id (int): The external job identifier (eg. CrowdFlower job ID)
    """
    data = get_job_data("crowdflower", external_job_id)
    metadata, _, completed_at, judged_qrps, full_json = data
    update_completed_job(db_conn, external_job_id, completed_at, metadata, full_json)
    add_judgments_for_qrps(db_conn, judged_qrps)


def main(args):
    """
    - Create a connection to RDS
    - Download result data from {crowdsource_platform} given external job ID
    - Insert results into various RDS tables

    Args:
        args (argparse.Namespace): input argument k:v pairs

    Returns:
        None
    """
    db_conn = psycopg2.connect(args.db_conn_str)

    fetch_and_write_job_results(db_conn, args.external_job_id)

    db_conn.commit()

if __name__ == "__main__":
    psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Take data from a crowdsourcing platform '
                                     'and upload it to a postgres database')

    parser.add_argument('external_job_id', help='External (eg. Crowdflower) job ID')

    parser.add_argument('-D', '--db_conn_str', required=True,
                        help='Database connection string')

    parser.add_argument('-p', '--platform', dest='crowdsource_platform',
                        default='crowdflower', choices=ALLOWED_CROWDSOURCE_PLATFORMS,
                        help='Crowdsourcing platform to get results from, \
                        default %(default)s, choices %(choices)s')

    args = parser.parse_args()

    main(args)
