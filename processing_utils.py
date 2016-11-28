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
    as not yet implemented. It will result in a warning being emmitted
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
    """Used for stat analysis and anomaly detection of the tweetter stream.
    Operates on two scales:
        1. local stat analysis per time frame
        2. global analysis over the accumulated stream data.
    """

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
        """ Performs basic anonmaly detection by comapring of the firist moment for a current iteration
        against the global accumulated statistic

        Args:
            htag_local_distribution: histogram of tag specific to the current time frame

        Returns:
            True if anomaly is detected
        """
        current_mean = StatAnalyzer.compute_first_moment(list(htag_local_distribution.values()))
        current_std = StatAnalyzer.compute_second_moment(list(htag_local_distribution.values()))
        running_mean, running_std = self._get_global_tag_moments()
        self._update_global_statistic(running_mean, running_std)  # TODO: this must be extracted from this call
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

    def _get_global_tag_moments(self) -> (float, float):
        """ Returns global mean and std of tag frequency over all accumulated history

        Returns:
            (float, float) that represents first and second global moments
        """
        avg_pipe = [{'$group':
                         {'_id': None,
                          'mean': {'$avg': '$local_tags_frequency_mean'}}}]
        std_pipe = [{'$group':
                         {'_id': None,
                          'mean': {'$avg': '$local_tags_frequency_std'}}}]

        def _compute_accumulated_std():
            try:
                avg_std = self.persistor.db.tweet_stats.aggregate(pipeline=std_pipe).get('result')[0].get('mean') or 0
            except AttributeError as e:
                avg_std = 0
            return np.sqrt(avg_std)

        try:
            global_tags_mean = self.persistor.db.tweet_stats.aggregate(pipeline=avg_pipe).get('result')[0].get(
                'mean') or 0
        except AttributeError as e:
            global_tags_mean = 0

        return global_tags_mean, _compute_accumulated_std()

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
    """Abstracts processing of a single tweet such as:
        1. tags extraction
        2. update of local tweet statistics
        3. update of global tweet statistics
    """

    def __init__(self, lang="en", persistor=None):
        self.lang = lang
        self.persistor = persistor or TweetPersistor()

    def check_language(self, data_json):
        return "lang" in data_json and data_json["lang"] == self.lang

    def process_tweet(self, data_json, htags):
        """ Basic tweet processing is done. Namely:
            1. tags extracted and used to update tag distribution for the current frame
            2. For historical reasons and potential off-line analysis all tweet is stored in db
            3. A global dictionary of tags distribution is updated.
            Due to the fact that number of tags grow almost linearly, global dictionary is stored in db

            Args:
                data_json: raw json with tweet data
                htags: reference to local to frame tag dictionary. This is NOT immutable. # TODO: might need to change
            """
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
    """DB access abstraction"""

    def __init__(self, client=None, db=None, host='localhost', port=27017):
        self.logger = logging.getLogger('persistor' + __name__)
        self.client = client or MongoClient(host, port)
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
        anomaly = self.db.tweet_stats.find({"anomaly": {"$exists": True}}) \
            .limit(1).sort("timestamp", pymongo.DESCENDING)
        return list(anomaly)[0].get('anomaly')
