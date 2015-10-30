import logging
import simplejson
import numpy as np
from datetime import datetime


class NoSuchJob(Exception):
    """
    Exception raised when specified job is not found in DB.
    """
    def __init__(self, msg):
        self.msg = msg

    @staticmethod
    def with_external_id(id):
        return NoSuchJob("No job found with external ID {} in DB".format(id))


class NoSuchGroup(Exception):
    """
    Exception raised when specified Group is not found in DB.
    """
    def __init__(self, msg):
        self.msg = msg

    @staticmethod
    def with_id(group_id):
        return NoSuchGroup("No group found with ID {} in DB".format(group_id))


def find_judged_qrps(db_conn):
    """
    Find all the previously judged query-result pairs in the Arcs DB.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database

    Returns:
        A set of query-result fxf pairs
    """
    query = "SELECT query, result_fxf FROM arcs_query_result WHERE judgment IS NOT NULL"

    with db_conn.cursor() as cur:
        cur.execute(query)
        return {(row[0], row[1]) for row in cur}


def insert_incomplete_job(db_conn, job):
    """
    Insert a new/incomplete job into the database.

    There is a UNIQUE constraint on external_id, so if we've already inserted a record w/ that ID,
    it will raise a DB exception.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        job (Job): An Arcs Job object

    Returns:
        A copy of the Job with its id field set by the DB
    """
    logging.info("Inserting job data into arcs_job")

    query = "INSERT INTO arcs_job (external_id, created_at, completed_at, platform, " \
            "metadata, results) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id"

    with db_conn.cursor() as cur:
        job_with_json_metadata = job._replace(metadata=simplejson.dumps(job.metadata))
        cur.execute(query, job_with_json_metadata[1:])  # exclude id, since we don't have one yet
        return job._replace(id=cur.fetchone()[0])


def insert_empty_group(db_conn, group):
    """
    Insert a new group into the database.

    This effectively persists metadata about a Group and returns a copy of the Group object
    with its id field populated.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        group (GroupDefinition): A definition for the new group

    Returns:
        A copy of the Group with its id field set by the DB
    """
    logging.info("Inserting group into arcs_group")

    query = "INSERT INTO arcs_group (created_at, name, description, params)" \
            "VALUES (%s, %s, %s, %s) RETURNING id"

    with db_conn.cursor() as cur:
        cur.execute(query, (datetime.utcnow(), group.name, group.description,
                            simplejson.dumps(group.params)))

        return group._replace(id=cur.fetchone()[0])


def update_completed_job(db_conn, external_job_id, completed_at, metadata, results):
    """
    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        external_job_id (str): A unique identifier for the external job
        completed_at (datetime): A datetime for when the job was completed
        metadata (dict): A job metadata dictionary
        results (dict): A job results dictionary

    Returns:
        A copy of the Job with its id field set by the DB
    """
    logging.info("Updating job with results in arcs_job")

    query = "UPDATE arcs_job SET completed_at=%s, metadata=%s, results=%s WHERE external_id=%s " \
            "RETURNING id"

    with db_conn.cursor() as cur:
        cur.execute(query, (completed_at, simplejson.dumps(metadata),
                            simplejson.dumps(results), str(external_job_id)))
        last_result = cur.fetchone()

        if not last_result:
            raise NoSuchJob.with_external_id(external_job_id)

        return last_result[0]


def _json_serialize(obj):
    """
    JSON serializer to ensure that numpy bools can be converted into valid JSON.
    """
    return str(bool(obj)) if isinstance(obj, np.bool_) else simplejson.dumps(obj)


def add_raw_group_results(db_conn, group_id, data):
    """
    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        group_id (int): A unique identifier for the group
        data (iterabe): An iterable of raw record dictionaries
    """
    logging.info("Inserting raw results data into arcs_group")

    update = "UPDATE arcs_group SET raw=%s WHERE id=%s"

    with db_conn.cursor() as cur:
        cur.execute(update, (simplejson.dumps(data, default=_json_serialize), group_id))


def insert_query(db_conn, group_id, query, domain=None):
    """
    Insert query and domain.

    If the pair already exists, simply return the ID of the existing pair.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        group_id (int): A unique integer identifier for the group of results
        query (str): A query string
        domain (str): Optional domain (eg. "data.cityofchicago.org")

    Returns:
        a query ID
    """
    with db_conn.cursor() as cur:
        insert = "INSERT INTO arcs_query (query, domain) " \
                 "SELECT %s, %s WHERE NOT EXISTS " \
                 "(SELECT id FROM arcs_query WHERE query=%s AND domain=%s) " \
                 "RETURNING id"

        cur.execute(insert, (query, domain, query, domain))
        last_result = cur.fetchone()

        if not last_result:
            cond_select = "SELECT id FROM arcs_query WHERE query=%s AND domain=%s"
            cur.execute(cond_select, (query, domain))
            last_result = cur.fetchone()

        query_id = last_result[0]

        insert2 = "INSERT INTO arcs_query_group_join (query_id, group_id) " \
                  "VALUES (%s, %s)"
        cur.execute(insert2, (query_id, group_id))

        return query_id


def insert_unjudged_query_results(db_conn, job_id, group_id, query_id, query, results):
    """
    Insert group data into the DB.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        job_id (int): A unique integer identifier for the job
        group_id (int): A unique integer identifier for the group of results
        query_id (int): A unique integer identifier for the query
        query (str): A query string
        results (iterable): A list of result dicts

    Returns:
        A pair containing the number of new QRPs inserted, and the number of redundant QRPs in the
        result set (this should be 0).
    """
    new_qrps_added = 0
    num_redundant_qrps = 0

    with db_conn.cursor() as cur:
        for result in results:
            insert1 = "INSERT INTO arcs_query_result (query, result_fxf, judgment, job_id, query_id) " \
                      "SELECT %s, %s, NULL, %s, %s WHERE NOT EXISTS " \
                      "(SELECT id FROM arcs_query_result WHERE query=%s AND result_fxf=%s) " \
                      "RETURNING id"

            conditional_select = "SELECT id FROM arcs_query_result WHERE query=%s AND result_fxf=%s"

            insert2 = "INSERT INTO arcs_group_join (group_id, query_result_id, result_position) " \
                      "SELECT %s, %s, %s WHERE NOT EXISTS " \
                      "(SELECT 1 FROM arcs_group_join WHERE group_id=%s AND query_result_id=%s " \
                      "AND result_position=%s) RETURNING 1"

            result_fxf = result["result_fxf"]
            result_position = result["result_position"]

            cur.execute(insert1, (query, result_fxf, job_id, query_id, query, result_fxf))

            last_result = cur.fetchone()

            if last_result:
                query_result_id = last_result[0]
                new_qrps_added += 1
            else:
                cur.execute(conditional_select, (query, result_fxf))
                query_result_id = cur.fetchone()[0]

            cur.execute(insert2, (group_id, query_result_id, result_position,
                                  group_id, query_result_id, result_position))

            last_result = cur.fetchone()

            if not last_result:
                num_redundant_qrps += 1

    return (new_qrps_added, num_redundant_qrps)


def insert_unjudged_data_for_group(db_conn, job_id, group_id, data):
    """
    Insert group data into the DB.

    This function goes through an iterable of dicts row-by-row adding each query, domain, and
    result set to the DB. It first upserts queries into `arcs_query` table, and from that
    operation a unique ID is returned. That ID is then inserted as a foreign key into the
    `arcs_query_result` table for each result in the result set. We track the number of new QRPs
    and redundant QRPs for debugging purposes.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        job_id (int): A unique integer identifier for the job
        group_id (int): A unique integer identifier for the group of results
        query_id (int): A unique integer identifier for the query
        data (iterable): An iterable of dicts
    """
    group_num_qrps_added = 0
    group_num_redundant_qrps = 0

    logging.info("Inserting group data into arcs_query_result and arcs_group_join")

    for row in data:
        query = row["query"]
        domain = row["domain"]

        # get query ID for this query, since it's referenced in arcs_query_result
        query_id = insert_query(db_conn, group_id, query, domain)

        results = row["results"]

        new_qrps_added, num_redundant_qrps = insert_unjudged_query_results(
            db_conn, job_id, group_id, query_id, query, results)

        group_num_qrps_added += new_qrps_added
        group_num_redundant_qrps += num_redundant_qrps

    logging.info("Added {} new QRPs to arcs_query_result".format(group_num_qrps_added))
    logging.warning("Observed {} redundant QRPs".format(group_num_redundant_qrps))


def add_judgments_for_qrps(db_conn, data):
    """
    Add judgments to the DB.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        data (iterable): An iterable of dicts
    """
    query = "UPDATE arcs_query_result SET judgment=%s, is_gold=%s WHERE query=%s AND result_fxf=%s"

    for row in data:
        with db_conn.cursor() as cur:
            cur.execute(query, (row["judgment"], row["_golden"], row["query"], row["result_fxf"]))


def query_ideals_query():
    """
    Get a SQL query that will return rows of query, domain, and aggregated judgments (as a sorted
    list) for all queries with non-NULL judgments.

    The ideal results are required for computing NDCG.

    Returns:
        A SQL query string
    """
    return "SELECT aq.query, domain, ARRAY_AGG(judgment ORDER BY judgment DESC) AS judgments " \
           "FROM arcs_query_result AS aqr LEFT JOIN arcs_query AS aq ON aqr.query_id=aq.id " \
           "WHERE judgment IS NOT NULL GROUP BY aq.query, aq.domain"


def group_queries_and_judgments_query(db_conn, group_id, group_type):
    """
    Get a SQL query that will return rows query, result fxf, result position, and judgment for all
    queries in group `group_id`.

    We order results by query and result position.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        group_id (int): A unique identifier for the group
        group_type (str): An identifier for group type (eg. "domain_catalog")

    Returns:
        A SQL query string
    """
    selects = ["aq.query", "result_fxf", "result_position", "judgment"]

    if group_type == 'domain_catalog':
        selects.append("domain")

    select_str = ', '.join(selects)

    # TODO: the use of aliases qj and gj makes this pretty hard to read; fix!
    query = "SELECT {} FROM arcs_query AS aq LEFT JOIN arcs_query_group_join AS qj " \
            "ON aq.id=qj.query_id " \
            "LEFT JOIN (SELECT * FROM arcs_query_result AS qr " \
            "LEFT JOIN arcs_group_join AS gj ON gj.query_result_id=qr.id WHERE gj.group_id=%s) " \
            "AS gj ON aq.id=gj.query_id " \
            "WHERE qj.group_id=%s ORDER BY query, result_position".format(select_str)

    with db_conn.cursor() as cur:
        query = cur.mogrify(query, (group_id, group_id))

    return query


def group_name(db_conn, group_id):
    """
    Get the name of an experimental group from its ID.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
        group_id (int): A unique identifier for the group

    Returns:
        A group name string
    """
    query = "SELECT name FROM arcs_group WHERE id=%s"

    with db_conn.cursor() as cur:
        cur.execute(query, (group_id,))
        result = cur.fetchone()
        if result:
            return result[0]
        else:
            raise NoSuchGroup.with_id(group_id)
