import numpy as np
from pymongo import MongoClient
import logging

from pymongo import errors


class StatAnalyzer:
    def __init__(self, running_mean=0, running_std=0, twt_peristor=None):
        self.running_mean = running_mean
        self.running_std = running_std
        self.persistor = twt_peristor or TweetPersistor()

    @staticmethod
    def compute_first_moment(values):
        if values:
            return np.mean(values)
        return 0

    @staticmethod
    def compute_second_moment(values):
        if values:
            return np.std(values)
        return 0

    def detect_local_anomaly(self, htag_local_distribution) -> bool:
        current_mean = StatAnalyzer.compute_first_moment(list(htag_local_distribution.values()))
        current_std = StatAnalyzer.compute_second_moment(list(htag_local_distribution.values()))
        if not self.running_mean - self.running_std <= current_mean <= \
                        self.running_mean + self.running_std:
            self._update_moments(current_mean, current_std)
            return False
        return True

    def get_global_tweet_mean(self):
        pass

    def get_global_tag_mean(self):
        pass

    def detect_global_anomaly(self, htag_local_distribution) -> bool:
        # I need a reference to db here in order to be able to access reference data
        # 1. The very first dummy thing to do is to compare the number of tweets
        # to the averaged historical number ot tweets
        # 2. If local anomaly detection detected the anomaly for some hashtag:
        #   compare local occurrences value to the global(if present) frequency
        # avg_tweets_per_frame =
        pass

    def _update_moments(self, *args):
        self.running_mean = args[0]
        self.running_std = args[1]


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


class TweetPersistor:
    def __init__(self, client=None, db=None):
        self.logger = logging.getLogger('persistor' + __name__)
        self.client = client or MongoClient('localhost', 27017)
        self.db = db or self.client.twtdb

    def insert_tweet(self, data):
        try:
            self.db.tweets.insert(data)
        except errors.PyMongoError as e:
            self.logger.error("data processing error %s" % e)

    def insert_statistics(self, data):
        try:
            self.db.tweet_stats.insert(data)
        except errors.PyMongoError as e:
            self.logger.error("data processing error %s" % e)

    def update_statistics(self, data):
        try:
            self.db.tweet_stats.update({"global_tags": 1}, {"$inc": data}, upsert=True)
        except errors.PyMongoError as e:
            self.logger.error("unable to update global tags dictionary", e)

    def get_tweets_count(self):
        return self.db.tweets.count()
