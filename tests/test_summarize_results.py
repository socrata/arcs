import unittest
import pandas as pd
from arcs.summarize_results import per_query_ndcg, stats


class SummarizeResultsTest(unittest.TestCase):
    def setUp(self):
        test_queries = ["2006", "2015", "291 erskine", "311", "330153"]

        test_domains = ["data.kcmo.org", "data.detroitmi.gov", "datacatalog.cookcountyil.gov",
                        "data.baltimorecity.gov", "data.medicare.gov"]

        data_df = pd.DataFrame({"query": test_queries,
                                "result_fxf": [None] * len(test_queries),
                                "result_position": [None] * len(test_queries),
                                "judgment": [None] * len(test_queries),
                                "raw_judgments": [[]] * len(test_queries),
                                "domain": test_domains})

        self.all_zero_result_query_df = data_df

        self.all_perfect_ideals_df = pd.DataFrame({"query": test_queries,
                                                   "domain": test_domains,
                                                   "judgments": [[3.0] * 10] * 5})

    def test_per_query_ndcg_on_all_empty_result_queries(self):
        data_df = self.all_zero_result_query_df
        data_df = data_df[data_df["result_fxf"].notnull()]
        ideals_df = self.all_perfect_ideals_df

        assert per_query_ndcg(data_df, ideals_df, 5) is None

    def test_stats_on_all_empty_result_queries(self):
        data_df = self.all_zero_result_query_df
        ideals_df = self.all_perfect_ideals_df

        assert stats(data_df, ideals_df)
