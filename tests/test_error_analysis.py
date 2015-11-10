import unittest
from arcs.error_analysis import get_dataset_url


class ErrorAnalysisTest(unittest.TestCase):
    def test_get_dataset_url(self):
        self.assertEqual(
            "https://data.foobar.com/d/1234-abcd",
            get_dataset_url("data.foobar.com", "1234-abcd", "dataset"))

        self.assertEqual(
            "https://data.foobar.com/stories/s/1234-abcd",
            get_dataset_url("data.foobar.com", "1234-abcd", "stories"))

        self.assertEqual(
            "https://data.foobar.com/view/1234-abcd",
            get_dataset_url("data.foobar.com", "1234-abcd", "datalenses"))

        self.assertEqual(
            "https://data.foobar.com/view/1234-abcd",
            get_dataset_url("data.foobar.com", "1234-abcd", "datalens_maps"))
