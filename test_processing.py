import unittest
from unittest import mock

from processing_utils import StatAnalyzer, TweetPersistor


class TestStatAnalyzer(unittest.TestCase):
    def setUp(self):
        self.stat_analyzer = StatAnalyzer()
        self.htags = {"tag1": 1, "tag2": 1.5}

    @mock.patch('processing_utils.StatAnalyzer._get_global_tag_moments', return_value=(1, 1))
    @mock.patch('processing_utils.StatAnalyzer._update_global_statistic', return_value=True)
    def test_detect_local_anomaly(self, _get_global_tag_moments, _update_global_statistic):
        self.assertEquals(self.stat_analyzer.detect_local_anomaly(self.htags), False)

    def test_detect_global_anomaly(self):
        pass

    def test_get_global_tweet_mean(self):
        pass

    def test_get_global_tag_mean(self):
        # Have to mock persistor response or insert syntetic data
        # self.stat_analyzer._get_global_tag_moments()
        pass


class TestTweetPersistor(unittest.TestCase):
    def setUp(self):
        self.persistor = TweetPersistor()

    def test_get_latest_anomaly(self):
        self.persistor.get_latest_anomaly()


if __name__ == '__main__':
    unittest.main()
