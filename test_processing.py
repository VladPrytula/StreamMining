import unittest

from processing_utils import StatAnalyzer


class TestStatAnalyzer(unittest.TestCase):
    def setUp(self):
        self.stat_analyzer = StatAnalyzer(1, 1)
        self.htags = {"tag1": 1, "tag2": 1.5}

    def test_detect_local_anomaly(self):
        self.assertEquals(self.stat_analyzer.detect_local_anomaly(self.htags), True)

    def test_detect_global_anomaly(self):
        self.assertEquals(False, True)


if __name__ == '__main__':
    unittest.main()
