import unittest

from processing_utils import StatAnalyzer


class TestStatAnalyzer(unittest.TestCase):
    def setUp(self):
        self.htags = {"tag1": 1, "tag2": 1}

    def test_detect_local_anomaly(self):
        self.assertEquals(StatAnalyzer.detect_local_anomaly(1, 1, self.htags), True)


if __name__ == '__main__':
    unittest.main()
