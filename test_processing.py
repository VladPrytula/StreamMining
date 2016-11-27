import unittest

from processing_utils import StatAnalyzer, TweetPersistor


class TestStatAnalyzer(unittest.TestCase):
    def setUp(self):
        self.stat_analyzer = StatAnalyzer(1, 1)
        self.htags = {"tag1": 1, "tag2": 1.5}

    def test_detect_local_anomaly(self):
        self.assertEquals(self.stat_analyzer.detect_local_anomaly(self.htags), True)

    def test_detect_global_anomaly(self):
        self.assertEquals(False, True)

    def test_get_global_tweet_mean(self):
        pass

    def test_get_global_tag_mean(self):
        # Have to mock persistor response or insert syntetic data
        self.stat_analyzer._get_global_tag_moments()


class TestTweetPersistor(unittest.TestCase):
    def setUp(self):
        self.persistor = TweetPersistor()


if __name__ == '__main__':
    unittest.main()
