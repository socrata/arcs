import os
import pandas as pd
from collections import namedtuple
from evaluation import dcg, is_statistically_significant


class Group(namedtuple("Group", ["name", "data"])):
    """
    A class to represent a Group in an Experiment.
    """
    def write_to_csv(self, filename=None):
        """
        Write Group data to a CSV.

        Args:
            filename: Optional path to the file destination to which the Group
                data should be written. If None (the default), the data is
                saved to a file called {self.name}.csv in the current directory.
        """
        filename = filename or "{}.csv".format(self.name)
        self.data.to_csv(filename, encoding="utf-8", index=False)

    @staticmethod
    def from_csv(filename, name=None):
        """
        Instantiate a Group from a CSV file of Group data.

        Args:
            filename: A string path to the file destination from which the Group
                data should be read.
            name: Optional string to use as the Group name. If None (the
                default), the basename of the specified file is used as the
                Group name.
        """
        name = name or os.path.splitext(os.path.basename(filename))[0]
        return Group(name, pd.read_csv(filename))

    def per_query_dcg(self, judged_results_df):
        """
        Compute DCG for each query in the Group, using the judgments in
        `judged_group_df`.

        Args:
            judged_results_df: A Pandas DataFrame required to have at a minimum
                a "judgment" column, and a "position" column.

        Returns: A Pandas DataFrame with a "query" column, and a "dcg" column
                of DCG scores.
        """
        def query_dcg(group_df):
            return dcg(group_df["judgment"], group_df["position"])

        grouped = judged_results_df.groupby("query", as_index=False)
        dcgs_df = pd.DataFrame({"query": grouped.first()["query"],
                                "dcg": grouped.apply(query_dcg)})

        return dcgs_df

    def stats(self, judged_results_df):
        """
        Generate stats for the group given the judgments in `judged_results_df`.

        Args:
            judged_results_df: A Pandas DataFrame required to have at a minimum
                a "judgment" column, and a "position" column.

        Returns: A dictionary of stats about the Group.
        """
        # join Group data with judged data
        judged_group_df = self.data.merge(
            judged_results_df, how="left", on=["domain", "query", "fxf"])

        # count the number of queries w/o judgments
        num_unjudged = len(judged_group_df[judged_group_df["judgment"].isnull()])

        # filter out un-judged queries
        judged_group_df = judged_group_df[judged_group_df["judgment"].notnull()]

        # compute DCG for each query
        dcgs_df = self.per_query_dcg(judged_group_df)

        return {
            "dcgs": list(dcgs_df["dcg"]),
            "avg_dcg": dcgs_df["dcg"].mean(),
            "num_irrelevant": len(judged_group_df[judged_group_df["judgment"] == 0]),
            "oddballs": list(dcgs_df[dcgs_df["dcg"] == 0]["query"]),
            "unjudged_qrps": num_unjudged
        }


def _count_num_diff(group1_df, group2_df):
    merged = group1_df[["domain", "query", "fxf", "position"]].merge(
        group2_df[["domain", "query", "fxf", "position"]],
        on=["domain", "query", "position"],
        suffixes=("_1", "_2"))

    return len(merged[merged["fxf_1"] != merged["fxf_2"]])


class Experiment(namedtuple("Experiment", ["group1", "group2"])):
    """
    A class to represent a relevance Experiment. An experiment consists of two
    groups. Typically, we will have a Group corresponding to a baseline system,
    and a Group corresponding to an experimental system.

    Args:
        judged_results_df: A Pandas DataFrame required to have at a minimum
            a "judgment" column, and a "position" column.

    Returns: A dictionary of experiment statistics.

    Sample output:

        {
            "diffs": 0,
            "baseline": {
                "avg_dcg": 2.3353196691022147,
                "unjudged_queries": 0,
                "num_irrelevant": 46,
                "oddballs": [
                    "sheet"
                ]
            },
            "disabled_row_level_matches": {
                "avg_dcg": 2.3353196691022147,
                "unjudged_queries": 0,
                "num_irrelevant": 46,
                "oddballs": [
                    "sheet"
                ]
            }
        }
    """
    def stats(self, judged_results_df, with_dcgs=False):
        group1_stats = self.group1.stats(judged_results_df)
        group2_stats = self.group2.stats(judged_results_df)
        num_diff = _count_num_diff(self.group1.data, self.group2.data)

        stats = {
            self.group1.name: group1_stats,
            self.group2.name: group2_stats,
            "diffs": num_diff
        }

        dcg_delta = group1_stats["avg_dcg"] - group2_stats["avg_dcg"]

        if dcg_delta != 0:
            is_significant, p = is_statistically_significant(
                group1_stats["dcgs"], group2_stats["dcgs"])

            stats.update({
                "statistically_significant": is_significant,
                "dcg_delta": dcg_delta,
                "p_value": p
            })

        if not with_dcgs:
            group1_stats.pop("dcgs")
            group2_stats.pop("dcgs")

        return stats
