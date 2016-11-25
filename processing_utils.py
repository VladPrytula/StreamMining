import numpy as np


class StatAnalyzer:
    @staticmethod
    def compute_average(values):
        return np.mean(values)

    @staticmethod
    def compute_deviation(values):
        return np.std(values)


class TweetProcessor:
    def __init__(self, lang="en"):
        self.lang = lang

    def check_language(self, data_json):
        return "lang" in data_json and data_json["lang"] == self.lang

    def process_tweet_content(self, data_json):
        pass

    def extract_hashtags(self, data_json):
        return data_json['entities']['hashtags']
