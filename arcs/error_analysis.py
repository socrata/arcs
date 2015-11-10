import psycopg2
import pandas as pd
from launch_job import cleanup_description
from db import group_queries_and_judgments_query

# NB: this code was pulled from es_load.py:
# https://github.com/socrata/cetera-etl/blob/master/src/etl/es_load.py#L49-L63)
# It is subject to change. Ideally, we would have a shared module for these types of data
# contracts. But things have been changing quickly. Let's revisit when ETL is more stable.
DATATYPE_MAPPING = {
    "datasets": ("dataset", ""),
    "datalenses": ("datalens", ""),
    "calendars": ("calendar", ""),
    "charts": ("chart", ""),
    "datalens_charts": ("chart", "datalens"),
    "files": ("file", ""),
    "forms": ("form", ""),
    "filters": ("filter", ""),
    "hrefs": ("href", ""),
    "geo_maps": ("map", "geo"),
    "tabular_maps": ("map", "tabular"),
    "datalens_maps": ("map", "datalens"),
    "pulses": ("pulse", ""),
    "stories": ("story", "")
}


OUTPUT_COLUMNS = ("query", "result_fxf", "result_position",
                  "name", "description", "url", "judgment")


def get_irrelevant_qrps(results):
    """
    Get a list of query-result pairs from the raw judgments where more than one
    worker gave the result a relevance score of 0.
    """
    error = "irrelevant"
    return [(d["query"], d["name"], d["link"], error, d["relevance"]["res"].count("0"))
            for d in results.itervalues() if d["relevance"]["res"].count("0") > 1]


def _get_max_source_tag(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT MAX(source_tag) FROM cetera_core_datatypes_snapshot")
        return cur.fetchone()[0]


def get_fxf_metadata_mapping(db_conn):
    """
    Get a dict mapping FXF to useful metadata about a dataset.

    Args:
        db_conn (psycopg2.extensions.connection): Connection to a database
            instance

    Returns:
        A dict of FXFs to dataset metadata
    """
    query = "SELECT nbe_fxf, datatype, domain_cname, unit_name AS name, " \
            "unit_desc AS description " \
            "FROM cetera_core_datatypes_snapshot WHERE source_tag = %s"

    with db_conn.cursor() as cur:
        source_tag = _get_max_source_tag(db_conn)
        cur.execute(query, (source_tag,))

        return {nbe_fxf: {
            "datatype": datatype,
            "domain_cname": domain_cname,
            "name": name,
            "description": description
        } for nbe_fxf, datatype, domain_cname, name, description in cur}


def get_dataset_url(domain, fxf, datatype):
    """
    Generate a permalink from the dataset's domain, fxf, and datatype.

    Args:
        domain (str): The domain of the dataset
        fxf (str): The fxf of the dataset
        datatype (tuple): The (view_type, display_type) of the dataset

    Returns:
        A dataset URL
    """
    dtype, vtype = DATATYPE_MAPPING.get(datatype, (None, None))

    if dtype == "story":
        url = "https://{}/stories/s/{}".format(domain, fxf)
    elif dtype == "datalens" or vtype == "datalens":
        url = "https://{}/view/{}".format(domain, fxf)
    else:
        url = "https://{}/d/{}".format(domain, fxf)

    return url


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Gather data from CrowdFlower judgments to server as '
        'the basis for error analysis')

    parser.add_argument('group_id', type=int,
                        help="The group whose judgments are the basis for analysis")
    parser.add_argument('-o', '--outfile', dest='outfile', type=str, required=True,
                        help='Name of CSV file to which data will be written.')
    parser.add_argument('-D', '--db_conn_str', required=True, help='Database connection string')
    parser.add_argument('--prod_db_conn_str', help='Optional connection string for prod DB')

    args = parser.parse_args()

    print "Reading metadata from prod. RDS"

    fxf_metadata_dict = get_fxf_metadata_mapping(
        psycopg2.connect(args.prod_db_conn_str or args.db_conn_str))

    db_conn = psycopg2.connect(args.db_conn_str)

    print "Reading all judged data for group"

    data_df = pd.read_sql(
        group_queries_and_judgments_query(db_conn, args.group_id, "domain_catalog"),
        db_conn)

    print "Counting irrelevants"

    data_df["num_irrelevants"] = data_df["raw_judgments"].apply(
        lambda js: sum([1 for j in js if j["judgment"] < 1]))

    data_df = data_df[data_df["num_irrelevants"] >= 2]

    print "Adding metadata to dataframe"

    data_df["metadata"] = data_df["result_fxf"].apply(
        lambda fxf: fxf_metadata_dict.get(fxf, {}))

    print "Extracting dataset names"

    data_df["name"] = data_df["metadata"].apply(
        lambda metadata: metadata.get("name"))

    print "Extracting and cleaning descriptions"

    data_df["description"] = data_df["metadata"].apply(
        lambda metadata: cleanup_description(metadata.get("description", "").decode("utf-8")))

    print "Extracting URLs"

    data_df["url"] = data_df.apply(
        lambda row: get_dataset_url(
            row["metadata"].get("domain_cname"),
            row["result_fxf"],
            row["metadata"].get("datatype")), axis=1)

    data_df.sort_values("judgment", inplace=True)

    outfile = args.outfile or "errors.csv"

    data_df.to_csv(outfile, encoding="utf-8", index=False, columns=OUTPUT_COLUMNS)
