import simplejson
from datetime import datetime
from pytz import utc
from collections import namedtuple


class Job(namedtuple("Job", ["id", "external_id", "created_at", "completed_at", "platform",
                             "metadata", "results"])):
    """
    A class to represent a crowdsourcing job for collecting relevance judgments.
    """
    def __new__(cls, **kwargs):
        return super(cls, Job).__new__(
            cls, kwargs.get("id"), kwargs["external_id"],
            kwargs.get("created_at", datetime.utcnow().replace(tzinfo=utc)),
            kwargs.get("completed_at"), kwargs.get("platform"),
            kwargs.get("metadata"), kwargs.get("results"))


class GroupDefinition(namedtuple("GroupDefinition", ["id", "created_at", "name", "description",
                                                     "params"])):
    """
    A class to represent a definition for a Group in an Experiment.
    """
    def __new__(cls, **kwargs):
        return super(cls, GroupDefinition).__new__(
            cls, kwargs.get("id"), kwargs.get("created_at", datetime.utcnow().replace(tzinfo=utc)),
            kwargs.get("name"), kwargs.get("description"), kwargs.get("params", {}))

    @staticmethod
    def from_json(s):
        return GroupDefinition(**simplejson.loads(s))


class Group(namedtuple("Group", ["definition", "judged_data"])):
    """
    A class to represent a Group in an Experiment.
    """
    pass


class Experiment(namedtuple("Experiment", ["group1", "group2"])):
    """
    A class to represent a relevance Experiment. An experiment consists of two
    groups. Typically, we will have a Group corresponding to a baseline system,
    and a Group corresponding to an experimental system.

    Args:
        judged_results_df: A Pandas DataFrame required to have at a minimum
            a "judgment" column, and a "result_position" column.

    Returns: A dictionary of experiment statistics.

    Sample output:

        {
            "diffs": 0,
            "baseline": {
                "avg_ndcg": 2.3353196691022147,
                "unjudged_queries": 0,
                "num_irrelevant": 46,
                "oddballs": [
                    "sheet"
                ]
            },
            "disabled_row_level_matches": {
                "avg_ndcg": 2.3353196691022147,
                "unjudged_queries": 0,
                "num_irrelevant": 46,
                "oddballs": [
                    "sheet"
                ]
            }
        }
    """
    pass
