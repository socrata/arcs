import unittest
import pandas as pd
from arcs.summarize_results import per_query_ndcg


class SummarizeResultsTest(unittest.TestCase):
    def test_per_query_ndcg_on_all_empty_result_queries(self):
        test_queries = ["2006", "2015", "291 erskine", "311", "330153"]

        test_domains = ["data.kcmo.org", "data.detroitmi.gov", "datacatalog.cookcountyil.gov",
                        "data.baltimorecity.gov", "data.medicare.gov"]

        data_df = pd.DataFrame({"query": test_queries,
                                "result_fxf": [None] * len(test_queries),
                                "result_position": [None] * len(test_queries),
                                "judgment": [None] * len(test_queries),
                                "raw_judgments": [None] * len(test_queries)})
        ideals_df = pd.DataFrame({"query": test_queries,
                                  "domain": test_domains,
                                  "judgments": [[3.0] * 10] * 5})

        assert per_query_ndcg(data_df, ideals_df, 5) is None
