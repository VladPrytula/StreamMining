import numpy as np


class StatAnalyzer:
    @staticmethod
    def compute_first_moment(values):
        return np.mean(values)

    @staticmethod
    def compute_second_moment(values):
        return np.std(values)

    @staticmethod
    def detect_local_anomaly(running_mean, running_std, htag_local_distribution):
        current_mean = StatAnalyzer.compute_first_moment(list(htag_local_distribution.values()))
        if not running_mean - running_std <= current_mean <= running_mean + running_std:
            return False
        return True

    def detect_global_anomaly(self, htag_local_distribution):
        pass


class TweetProcessor:
    def __init__(self, lang="en"):
        self.lang = lang

    def check_language(self, data_json):
        return "lang" in data_json and data_json["lang"] == self.lang

    def process_tweet(self, data_json, htags):
        pass

    def extract_hashtags(self, data_json):
        return data_json['entities']['hashtags']

    def build_local_tag_distribution(self, data_json, htags):
        for tag in self.extract_hashtags(data_json):
            htags[tag['text']] += 1


class FrameProcessor:
    pass
