import unittest
import pandas as pd
from math import log
from arcs.evaluation import ndcg, dcg
from numpy import cumsum


class EvaluationTest(unittest.TestCase):
    def test_ndcg(self):
        """
        Create a DataFrame from dummy data to ensure correctness. This uses
        the data given here [in Wikipedia]
        (https://en.wikipedia.org/wiki/Discounted_cumulative_gain#Example),
        but we use the standard NDCG variant described there.
        """

        df = pd.DataFrame({"i": range(6), "rel_i": [3, 2, 3, 0, 1, 2]})
        df["gain"] = 2**df["rel_i"] - 1
        df["discount"] = df["i"].apply(lambda x: log(x + 2, 2))
        df["dg"] = df["gain"] / df["discount"]
        df["dcg"] = cumsum(df["dg"])
        df["ideal_gain"] = 2**pd.Series(sorted(df["rel_i"], reverse=True)) - 1
        df["ideal_dg"] = df["ideal_gain"] / df["discount"]
        df["idcg"] = cumsum(df["ideal_dg"])
        df["ndcg"] = df["dcg"] / df["idcg"]

        print df

        # test DCG w/ explicit indices
        self.assertEqual(df.iloc[-1]["dcg"], dcg(df["rel_i"], df["i"]))

        # test NDCG w/ explicit indices
        self.assertEqual(df.iloc[-1]["ndcg"],
                         ndcg(df["rel_i"], indices=df["i"]))

        # test NDCG w/ implicit indices
        self.assertEqual(df.iloc[-1]["ndcg"], ndcg(df["rel_i"]))

        # update ideal scores to be all 3s (ie. "perfectly relevant")
        df["ideal_rel_i"] = [3] * 6
        df["ideal_gain"] = 2**df["ideal_rel_i"] - 1
        df["ideal_dg"] = df["ideal_gain"] / df["discount"]
        df["idcg"] = cumsum(df["ideal_dg"])
        df["ndcg"] = df["dcg"] / df["idcg"]

        print df

        # test NDCG w/ explicit indices and ideal judgments
        self.assertEqual(df.iloc[-1]["ndcg"],
                         ndcg(df["rel_i"], df["i"], df["ideal_rel_i"]))

        # test that NDCGs differ when ideal judgments differ
        self.assertNotEqual(df.iloc[-1]["ndcg"], ndcg(df["i"]))
