import datetime
import warnings

import numpy as np
import time

import pymongo
from pymongo import MongoClient
import logging

from pymongo import errors


def not_implemented(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emmitted
    when the function is used."""

    def newFunc(*args, **kwargs):
        warnings.warn("Call to non_implemented function %s." % func.__name__,
                      category=DeprecationWarning)
        return func(*args, **kwargs)

    newFunc.__name__ = func.__name__
    newFunc.__doc__ = func.__doc__
    newFunc.__dict__.update(func.__dict__)
    return newFunc


class StatAnalyzer:
    def __init__(self, twt_peristor=None):
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
        running_mean, running_std = self._get_global_tag_moments()
        self._update_global_statistic(running_mean, running_std)
        if not running_mean - running_std <= current_mean + current_std <= running_mean + running_std:
            self.persistor.db.tweet_stats.insert({'anomaly': dict(htag_local_distribution),
                                                  'timestamp': datetime.datetime.utcnow()})
            return True
        return False

    def _update_global_statistic(self, running_mean, running_std):
        self.persistor.update_statistics({'global_tag_mean': running_mean,
                                          'global_tag_std': running_std,
                                          'global_tweets_mean': 'not_implemented',
                                          'global_tweets_std': 'not_implemented'},
                                         'global_moments', "$set")

    @not_implemented
    def _get_global_tweet_moments(self):
        pass

    def _get_global_tag_moments(self):
        """this returns tag moments over time frames over all time-line"""
        # TODO: must be extracted to TweetPersistor
        avg_pipe = [{'$group':
                         {'_id': None,
                          'mean': {'$avg': '$local_tags_frequency_mean'}}}]
        std_pipe = [{'$group':
                         {'_id': None,
                          'mean': {'$avg': '$local_tags_frequency_std'}}}]

        def _compute_accumulated_std():
            avg_std = self.persistor.db.tweet_stats.aggregate(pipeline=std_pipe).get('result')[0].get('mean') or 0
            return np.sqrt(avg_std)

        global_tags_mean = self.persistor.db.tweet_stats.aggregate(pipeline=avg_pipe).get('result')[0].get('mean') or 0
        global_tags_std = _compute_accumulated_std()

        return global_tags_mean, global_tags_std

    @not_implemented
    def detect_global_anomaly(self, htag_local_distribution) -> bool:
        # I need a reference to db here in order to be able to access reference data
        # 1. The very first dummy thing to do is to compare the number of tweets
        # to the averaged historical number ot tweets
        # 2. If local anomaly detection detected the anomaly for some hashtag:
        #   compare local occurrences value to the global(if present) frequency
        # avg_tweets_per_frame =
        pass


class TweetProcessor:
    def __init__(self, lang="en", persistor=None):
        self.lang = lang
        self.persistor = persistor or TweetPersistor()

    def check_language(self, data_json):
        return "lang" in data_json and data_json["lang"] == self.lang

    def process_tweet(self, data_json, htags):
        self.update_local_tag_distribution(data_json, htags)
        self.persistor.insert_tweet(data_json)
        self.update_global_tags_distribution(htags)

    def extract_hashtags(self, data_json):
        return data_json['entities']['hashtags']

    def update_local_tag_distribution(self, data_json, htags):
        for tag in self.extract_hashtags(data_json):
            htags[tag['text']] += 1

    def update_global_tags_distribution(self, local_htag_distribution):
        if local_htag_distribution:
            self.persistor.update_statistics(dict(local_htag_distribution), "global_tags", "$inc")


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

    def update_statistics(self, data, key, command):
        try:
            self.db.tweet_stats.update({key: 1}, {command: data}, upsert=True)
        except errors.PyMongoError as e:
            self.logger.error("unable to update statistics", e)

    def get_tweets_count(self):
        return self.db.tweets.count()

    def get_latest_anomaly(self):
        anomaly = self.db.tweet_stats.find({"anomaly": {"$exists": True}})\
            .limit(1).sort("timestamp", pymongo.DESCENDING)
        print(list(anomaly)[0].get('anomaly'))