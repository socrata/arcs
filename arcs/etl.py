from dateutil.parser import parse as dtparse
import argparse
import crowdflower
import datetime
import json
import logging
import os
import psycopg2
import psycopg2.extras
import re
import requests
import StringIO
import sys
import time
import zipfile


HERE = os.path.dirname(os.path.abspath(__file__))
# TODO: if we ever allow more platforms, we'll have to add API
# wrappers for them, and then add them to this list
ALLOWED_CROWDSOURCE_PLATFORMS = frozenset(('crowdflower',))
FXF_RE = re.compile(r'[a-z0-9]{4}-[a-z0-9]{4}$')


def create_tables(conn, sql_file=None):
    """
    Run the table initialization SQL contained in sql_file

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


def create_db_connection(db_name, db_user, db_host):
    """
    Create a connection to a postgres database

    Args:
        db_name (str): name of the postgres DB
        db_user (str): name of the postgres user
        db_host (str): name of the postgres host

    Returns:
        conn (psycopg2.extensions.connection): Connection to the DB
    """
    logging.info("Creating connection to database with parameters: "
                 "dbname={} user={} host={}".format(db_name,
                                                    db_user,
                                                    db_host))
    conn = psycopg2.connect("dbname={} user={} host={}".format(
        db_name, db_user, db_host))
    return conn


def get_crowdflower_metadata(external_job_id, api_key):
    """
    Get job metadata from CrowdFlower
    TODO: do this in vanilla requests.
        Haven't figured out the URL to hit, need to read
        the python-crowdflower code...

    Args:
        external_job_id (str or int): CrowdFlower job ID
        api_key (str): CrowdFlower API key (see README)

    Returns:
        metadata (dict): all available metadata for this job
        created_at (datetime.datetime): time of job creation
        completed_at (datetime.datetime or None): time of job completion,
            if completed, else None

    """
    conn = crowdflower.Connection(api_key=api_key)
    metadata = conn.job(external_job_id).properties
    created_at = dtparse(metadata.get('created_at'))
    completed_at = metadata.get('created_at')
    if completed_at:
        completed_at = dtparse(completed_at)
    return metadata, created_at, completed_at


def get_crowdflower_data(external_job_id, api_key=None):
    """
    Get job data from CrowdFlower

    Args:
        external_job_id (str or int): CrowdFlower job ID
        api_key (str): CrowdFlower API key (see README)

    Returns:
        qrps (dict): dict of (query, fxf): (judgment,
            created_at, last_updated_at) tuples
        data (dict): dict of (query, fxf): (result_postition,
            json_blob) tuples
        full_json (dict): the full return content from
            crowdflower, keyed by line number

    """
    api_key = api_key or os.environ["CROWDFLOWER_API_KEY"]
    post_url = 'https://api.crowdflower.com/v1/jobs/{job_id}/regenerate' \
               '?type=json&key={api_key}'
    filled_post_url = post_url.format(job_id=external_job_id, api_key=api_key)
    ret = requests.post(filled_post_url)
    ret.raise_for_status()
    logging.info("Waiting 5 seconds because CrowdFlower doesn't like getting "
                 "too many requests at once...")
    time.sleep(5)

    get_url = 'https://api.crowdflower.com/v1/jobs/{job_id}.csv' \
              '?type=json&key={api_key}'
    filled_get_url = get_url.format(job_id=external_job_id, api_key=api_key)

    for _ in range(5):
        try:
            ret = requests.get(filled_get_url)
            # we are returned a bytestring that would be a zipfile, were it a file
            # containing a single file where each line is a json blob
            zc = zipfile.ZipFile(StringIO.StringIO(ret.content))
            zip_data = zc.open(zc.namelist()[0])
            if zip_data:
                break
        except zipfile.BadZipfile:
            logging.error('Waiting 5 seconds, trying to grab zipfile again...')
            time.sleep(5)

    qrps, data, full_json = extract_json_from_csv(zip_data)
    return qrps, data, full_json


def extract_json_from_csv(zip_data):
    """
    Helper function to grab the individual lines of JSON
    from the csv that CrowdFlower returns

    Args:
        zip_data (zipfile.ZipExtFile): opened zipfile or
             other iterable full of JSONifyable strings

    Returns:
        qrps (dict): dict of (query, fxf): (judgment,
            created_at, last_updated_at) tuples
        data (dict): dict of (query, fxf): (result_postition,
            json_blob) tuples
        full_json (dict): the full return content from
            crowdflower, keyed by line number

    We're turning the iterable into a dictionary/JSON, so that
    we can stash it in a JSON-type field in our RDS

    """
    qrps = {}
    data = {}
    full_json = {}
    for i, line in enumerate(zip_data):
        j = json.loads(line)
        full_json[i] = j
        dat = j.get('data')
        query = dat.get('query')
        # we should always include result_fxf in the data we hand off
        # to CrowdFlower, so that we don't have to parse it out
        # of the URL (but we can do that if necessary)
        fxf = dat.get('result_fxf') or FXF_RE.search(dat.get('link')).group()
        judgment = j['results']['relevance'].get('avg')
        result_position = dat.get('result_position')
        created_at = j.get('created_at')
        last_updated_at = j.get('updated_at')
        qrps[(query, fxf)] = (judgment, created_at, last_updated_at)
        data[(query, fxf)] = (result_position, j)

    return qrps, data, full_json


def get_job_data(crowdsource_platform, external_job_id, api_key=None):
    """
    Get job metadata and data from the platform

    Args:
        crowdsource_platform (str): name of the crowdsourcing
            platform (eg. crowdflower)
        external_job_id (str): id of the job, according to the
            platform

    Returns:
        metadata (dict): metadata blob (eg. prompt, HTML...)
        job_created_at (datetime): time of job creation
        job_completed_at (datetime or None): time of job completion,
            if completed, else None
        qrps (dict): dict of (query, fxf): (judgment,
            created_at, last_updated_at) tuples
        data (dict): dict of (query, fxf): (result_postition,
            json_blob) tuples
        full_json (dict): the full return content from
            crowdflower, keyed by line number

    """
    logging.info("Gathering data and metadata from {}".format(crowdsource_platform))
    if crowdsource_platform == 'crowdflower':
        api_key = api_key or os.environ['CROWDFLOWER_API_KEY']
        metadata, job_created_at, job_completed_at = get_crowdflower_metadata(
            external_job_id, api_key)
        qrps, data, full_json = get_crowdflower_data(external_job_id, api_key)
    else:
        logging.error('Unexpected crowdsource_platform {}, \
        must be one of {}'.format(crowdsource_platform,
                                  ALLOWED_CROWDSOURCE_PLATFORMS))
        sys.exit(1)

    return metadata, job_created_at, job_completed_at, qrps, data, full_json


def post_to_arcs_job(conn, group_id, external_id, job_created_at,
                     job_completed_at, crowdsource_platform,
                     job_type, metadata, job_data, notes_file):
    """
    Update the arcs_job table, and if necessary the arcs_group table

    ARCS_GROUP
    - if group_id < 0: creates a new group with an auto-incremented key
    - if group_id >= 0:
        - if group already exists: noop
        - if group does not exist: create group with that ID

    ARGS_JOB
    - adds a single entry to arcs_job detailing job, if does not exist

    Args:
        conn (psycopg2.extensions.connection): Connection to the DB
        group_id (int): value of arcs_group(id) to add this job to
            if less than 0, creats a new group
        external_id (str): platform ID of job
        job_created_at (datetime.datetime): job creation time
        job_completed_at (datetime.datetime): job completion time
        crowdsouce_platform (str): name of the crowdsource platform,
            eg. crowdflower
        job_type (str): name of the job type (eg. domain, odn)
        metadata (json): json blob of job metadata
        job_data (json): full json blob representing all returned data
        notes_file (str -- filepath): path to a file containing notes about
            this group, if creating a new one


    Returns:
        job_id (int): ID of the job just entered
        group_id (int): ID of the group just entered
    """

    cur = conn.cursor()

    cur.execute('SELECT * from arcs_group where '
                'arcs_group.id = %s;', (group_id,))
    group_exists = bool(cur.fetchone())

    # post to arcs_group if necessary
    if group_id < 0 or not group_exists:
        if notes_file and os.path.exists(notes_file):
            group_desc = open(notes_file).read().strip()
        else:
            logging.warning('Inserting empty description into group, '
                            'you can add one later if you\'d like')
            group_desc = ''
        if group_id < 0:
            cur.execute("INSERT INTO arcs_group (created_at, description)"
                        "VALUES (%s, %s) RETURNING id;",
                        (datetime.datetime.now(), group_desc))
            group_id = cur.fetchone()[0]
            logging.info("Created new group {}".format(group_id))
        else:
            cur.execute("INSERT INTO arcs_group (id, created_at, description)"
                        "VALUES (%s, %s, %s);",
                        (group_id, datetime.datetime.now(), group_desc))
            logging.info('Created group {} with timestamp '.format(group_id) +
                         '{} and description {}'.format(datetime.datetime.now(),
                                                        group_desc))
        conn.commit()
    logging.info("Current group_id is {}".format(group_id))

    # post to arcs_job
    logging.info("Inserting data into arcs_job")
    try:
        cur.execute("INSERT INTO arcs_job (external_id, created_at, completed_at, "
                    "platform, job_type, metadata, results) VALUES (%s, %s, %s, %s,"
                    " %s, %s, %s) RETURNING id;", (external_id, job_created_at,
                                                   job_completed_at, crowdsource_platform,
                                                   job_type, metadata, job_data))
        job_id = cur.fetchone()[0]
    except (psycopg2.IntegrityError, psycopg2.InternalError):
        logging.error('Job {} exists'.format(external_id))
        conn.rollback()
        cur.execute("SELECT id FROM arcs_job WHERE "
                    "external_id = %s", (external_id,))
        job_id = cur.fetchone()[0]

    conn.commit()
    return job_id, group_id


def post_to_arcs_group_join(conn, group_id, qrp_keys, data):
    """
    Update the arcs_group_join table

    For each (query, fxf) pair, add an entry into the arcs_group_join table
    *WARNING* (group_id, qrp_id, result_pos) is unique, and if it already exists,
    this won't update the payload, because working around the lack of UPSERT is
    hard. This shouldn't really come up...(#lastwords)

    Args:
        conn (psycopg2.extensions.connection): Connection to the DB
        group_id (int): value of arcs_group(id)
        qrp_keys (dict): (query, fxf): id dictionary
        data (dict): (query, fxf): data dictionary

    Returns:
        None
    """
    logging.info("Inserting data into arcs_group_join")
    cur = conn.cursor()
    for (query, fxf), (result_position, json_blob) in data.iteritems():
        qrp_id = qrp_keys.get((query, fxf))
        try:
            cur.execute("INSERT INTO arcs_group_join (group_id, query_result_id, "
                        "res_position, payload) VALUES (%s, %s, %s, %s);",
                        (group_id, qrp_id, result_position, json_blob))
        except (psycopg2.IntegrityError, psycopg2.InternalError):
            logging.error('({}, {}, {}) exists'.format(group_id,
                                                       qrp_id, result_position))

    conn.commit()


def post_to_arcs_query_result(conn, qrps, job_id):
    """
    Update the arcs_query_result table

    Add (query, result, judgment, job_id) to arcs_query_result.
    *WARNING* (query, result) is unique, so on conflict, the old judgment
    stays, same as with post_to_arcs_group above.

    Args:
        qrps (dict): dict of (query, fxf): (judgment,
            created_at, last_updated_at) tuples

    Returns:
        qrp_keys (dict): dict of (query, result_fxf): query_result_id

    """
    logging.info("Inserting data into arcs_query_result")
    cur = conn.cursor()
    qrp_keys = {}
    for (query, fxf), (judgment, created_at, updated_at) in qrps.iteritems():
        try:
            cur.execute("INSERT INTO arcs_query_result (query, result_fxf, judgment, job_id) "
                        "VALUES (%s, %s, %s, %s) RETURNING id;", (query, fxf, judgment, job_id))
            qrp_id = cur.fetchone()[0]
            qrp_keys[(query, fxf)] = qrp_id
        except (psycopg2.IntegrityError, psycopg2.InternalError):
            logging.error('({}, {}) exists'.format(query, fxf))
            conn.rollback()
            cur.execute("SELECT id FROM arcs_query_result WHERE query = %s AND "
                        "result_fxf = %s", (query, fxf))
            qrp_id = cur.fetchone()[0]
            qrp_keys[(query, fxf)] = qrp_id

    conn.commit()
    return qrp_keys


def arg_parser():
    parser = argparse.ArgumentParser(description='Take data from a crowdsourcing platform '
                                     'and upload it to a postgres database')

    # TODO: add the ability pass in a csv for pruning (of existing QRPs)
    # and upload to platform

    parser.add_argument('-j', '--job_id', dest='external_job_id', required=True,
                        help='External (eg. Crowdflower) job ID')
    parser.add_argument('-p', '--platform', dest='crowdsource_platform',
                        default='crowdflower', choices=ALLOWED_CROWDSOURCE_PLATFORMS,
                        help='Crowdsourcing platform to get results from, \
                        default %(default)s, choices %(choices)s')
    parser.add_argument('-t', '--job_type', dest='job_type', default='domain',
                        choices=['domain', 'odn'],
                        help='Crowdsourcing job type, \
                        default %(default)s, choices %(choices)s')
    parser.add_argument('-n', '--notes_file', dest='notes_file', default=None,
                        help='Optional file containing notes about this group, \
                        default %(default)s')
    parser.add_argument('-H', '--db_host', dest='db_host', default='localhost',
                        help='Database hostname, default %(default)')
    parser.add_argument('-u', '--db_user', dest='db_user', required=True,
                        help='Database username')
    parser.add_argument('-d', '--db', dest='db_name', required=True,
                        help='Database name')
    parser.add_argument('-s', '--skip', dest='skip_create_tables',
                        action='store_true', default=False,
                        help='Whether to skip table creation (which should \
                        be a CREATE TABLE IF NOT EXISTS, but just in case)')
    parser.add_argument('-g', '--group', dest='group_id',
                        type=int, default=-1,
                        help='The group ID to add this job to. If unspecified \
                        or less than 0, will create a new group')

    args = parser.parse_args()
    return args


def main(args):
    """
    - Create a connection to RDS
    - Optionally create the tables, if they don't exist yet
    - Download result data from {crowdsource_platform}
    - Insert results into various RDS tables

    Args:
        args (argparse.Namespace): input argument k:v pairs

    Returns:
        None
    """
    conn = create_db_connection(args.db_name, args.db_user, args.db_host)
    if not args.skip_create_tables:
        create_tables(conn)
    ret = get_job_data(args.crowdsource_platform,
                       args.external_job_id)
    metadata, job_created_at, job_completed_at, qrps, data, full_json = ret
    job_id, group_id = post_to_arcs_job(conn, args.group_id, args.external_job_id,
                                        job_created_at, job_completed_at,
                                        args.crowdsource_platform, args.job_type,
                                        metadata, full_json, args.notes_file)

    qrp_keys = post_to_arcs_query_result(conn, qrps, job_id)
    post_to_arcs_group_join(conn, group_id, qrp_keys, data)
    conn.close()


if __name__ == "__main__":
    psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
    logging.basicConfig(level=logging.INFO)
    args = arg_parser()
    main(args)
