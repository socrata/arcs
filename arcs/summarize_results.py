import pandas as pd
import psycopg2
import simplejson
from evaluation import dcg, ndcg
from evaluation import is_statistically_significant
from db import group_queries_and_judgments_query, query_ideals_query, group_name


def _count_num_diff(group1_df, group2_df):
    merged = pd.merge(group1_df[["domain", "query", "result_fxf", "result_position"]],
                      group2_df[["domain", "query", "result_fxf", "result_position"]],
                      on=["domain", "query", "result_position"],
                      suffixes=("_1", "_2"))

    total_differences = len(merged[merged["result_fxf_1"] != merged["result_fxf_2"]])

    # number of unique qrps; should be the same between the two groups, assuming that the number of
    # queries and results for each query were the same;
    group_1_qrps = {(row[0], row[1], row[2]) for row in group1_df.to_records(index=False)}
    group_2_qrps = {(row[0], row[1], row[2]) for row in group2_df.to_records(index=False)}
    num_qrps_diff = len(group_1_qrps - group_2_qrps)

    return (total_differences, num_qrps_diff)


def per_query_ndcg(data, ideals, ndcg_at):
    """
    Compute NDCG for each query in data.

    Args:
        data (pandas.DataFrame): A Pandas DataFrame required to have at a minimum "query",
            "result_position", and "judgment" columns
        ideals (pandas.DataFrame): A Pandas DataFrame with a "query" column and a "judgments"
            column containing the ideal judgments at each position

    Returns: A Pandas DataFrame with a "query" column, and a "dcg" column
            of DCG scores.
    """
    def trim(group_df):
        return group_df[group_df["result_position"] < ndcg_at]

    def query_dcg(group_df):
        data = trim(group_df)
        score = dcg(data["judgment"], data["result_position"]) if len(data) > 0 else 0.0
        return score

    def query_ndcg(group_df):
        data = trim(group_df)
        score = ndcg(data["judgment"],
                     indices=data["result_position"],
                     ideal_judgments=data["ideals"].iloc[0][:ndcg_at]) if len(data) > 0 else 0.0

        return score

    ideals = ideals.rename(columns={"judgments": "ideals"}, inplace=False)
    data = data.merge(ideals, on=["query", "domain"])
    grouped = data.groupby(["query", "domain"], as_index=False)

    dcgs_df = pd.DataFrame({"query": grouped.first()["query"],
                            "domain": grouped.first()["domain"],
                            "dcg": grouped.apply(query_dcg).reset_index()[0],
                            "ndcg": grouped.apply(query_ndcg).reset_index()[0]})

    return dcgs_df


def find_oddballs(judged_data):
    """
    TODO: We really want the raw judgments to do this, but that data is not readily available yet,
    so as a first pass, we'll just look at all results whose aggregated judgment is less than 1.
    """
    return judged_data[judged_data["judgment"] < 1][["query", "domain", "result_fxf", "result_position"]].to_records()


def group_ndcgs(judged_data, ideals, ndcg_at):
    # filter out un-judged queries
    judged_group_df = judged_data[judged_data["judgment"].notnull()]

    # filter out QRPs with "something went wrong" judgments
    judged_group_df = judged_data[judged_data["judgment"] >= 0]

    # group results by query and domain
    grouped = judged_data.groupby(["query", "domain"])

    grouped = pd.DataFrame.from_records(
        [(query, domain, list(group["judgment"])) for ((query, domain), group) in grouped],
        columns=["query", "domain", "judgments"])

    # compute NDCG for each query
    ndcgs_df = per_query_ndcg(judged_group_df, ideals, ndcg_at)

    return ndcgs_df


def per_domain_ndcgs(judged_data, ideals, ndcg_at):
    ndcgs_df = group_ndcgs(judged_data, ideals, ndcg_at)
    grouped = ndcgs_df.groupby("domain")
    return grouped.mean().reset_index()


def precision(judged_data):
    """
    Our relevance judgments are on a graded scale of 0-3, where scores of 1-3 are considered
    relevant, and less than 1 is irrelevant. We compute precision of the result set based on
    this quanitization.

    Args:
        judged_data (pandas.DataFrame): A DataFrame with at a minimum query, domain, judgment,
            result_fxf, and result_position columns

    Returns:
        A floating point value corresponding to the precision of the result set.
    """
    # filter out un-judged queries
    judged_group_df = judged_data[judged_data["judgment"].notnull()]

    # filter out QRPs with "something went wrong" judgments
    judged_group_df = judged_data[judged_data["judgment"] >= 0]

    return len(judged_data[judged_data["judgment"] >= 1]) / float(len(judged_data))


def stats(judged_data, ideals):
    """
    Get summary statistics for a particular group of query-result pairs.

    We report the following:
        num_queries: the number of queries in the group
        unjudged_qrps: the number of query-result pairs without a judgment
        ndcg_error: the NDCG error (1 - avg_ndcg_at_5)
        zero_result_queries: the number of queries in the group with no results
        num_irrelevant: the number of query-result pairs judged to be irrelevant

    Args:
        judged_data (pandas.DataFrame): A DataFrame with at a minimum query, domain, judgment,
            result_fxf, and result_position columns
        ideals (pandas.DataFrame): A DataFrame of ideal judgments for each query

    Returns:
        A dict of group summary statistics
    """
    # get the queries w/ zero results
    zero_result_queries = judged_data[judged_data["result_fxf"].isnull()]

    # count the number of remaining queries w/o judgments
    num_queries = len(judged_data.groupby(["query", "domain"]))
    judged_data = judged_data[judged_data["result_fxf"].notnull()]
    num_unjudged = len(judged_data[judged_data["judgment"].isnull()])

    # find QRPs with more than one 0 or -1 judgment
    oddballs = [list(x) for x in find_oddballs(judged_data)]

    # compute NDCG for each query
    ndcgs_at_5 = group_ndcgs(judged_data, ideals, 5)
    ndcgs_at_10 = group_ndcgs(judged_data, ideals, 10)
    mean_ndcg_at_5 = ndcgs_at_5["ndcg"].mean()
    mean_ndcg_at_10 = ndcgs_at_10["ndcg"].mean()

    return {
        "num_queries": num_queries,
        "unjudged_qrps": num_unjudged,
        "avg_ndcg_at_5": mean_ndcg_at_5,
        "avg_ndcg_at_10": mean_ndcg_at_10,
        "ndcg_error": 1 - mean_ndcg_at_5,
        "precision": precision(judged_data),
        "num_zero_result_queries": len(zero_result_queries),
        "num_irrelevant": len(oddballs),
    }


def is_stat_sig(group_1_ndcgs, group_2_ndcgs):
    merged = pd.merge(group_1_ndcgs, group_2_ndcgs, on=["query", "domain"], suffixes=('_1', '_2'))
    return is_statistically_significant(merged["ndcg_1"], merged["ndcg_2"])


def main(db_conn_str, group_1_id, group_2_id):
    """
    - get group 1 data
    - get group 2 data
    - get stats for each group
    - compare groups
    """
    db_conn = psycopg2.connect(db_conn_str)

    experiment_stats = {}
    ideals_df = pd.read_sql(query_ideals_query(), db_conn)
    group_data = []

    groups = [group_1_id, group_2_id]
    groups = [(group_id, group_name(db_conn, group_id)) for group_id in groups]
    ndcgs = []

    for group_id in [group_1_id, group_2_id]:
        data_df = pd.read_sql(
            group_queries_and_judgments_query(db_conn, group_id, "domain_catalog"),
            db_conn)

        name = group_name(db_conn, group_id) + " " + str(group_id)

        group_data.append(data_df)
        group_stats = stats(data_df, ideals_df)
        experiment_stats.update({name: group_stats})
        ndcgs.append(group_stats["avg_ndcg_at_5"])

    total_differences, unique_qrps = _count_num_diff(group_data[0], group_data[1])
    experiment_stats["num_total_diffs"] = total_differences
    experiment_stats["num_unique_qrps"] = unique_qrps
    experiment_stats["ndcg_delta"] = (ndcgs[1] - ndcgs[0])

    print simplejson.dumps(experiment_stats, indent=4 * ' ')


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Report summary statistics on groups from relevance experiment')

    parser.add_argument('group_1_id', type=int,
                        help='Identifier for baseline group')

    parser.add_argument('group_2_id', type=int,
                        help='Identifier for experimental group')

    parser.add_argument('-D', '--db_conn_str', required=True,
                        help='Database connection string')

    args = parser.parse_args()

    main(args.db_conn_str, args.group_1_id, args.group_2_id)
