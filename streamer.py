from datetime import datetime

import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from config_api import *
import json
from pymongo import MongoClient
import time
import numpy as np


class StatAnalyzer:
    @staticmethod
    def compute_average(values):
        return np.mean(values)

    @staticmethod
    def compute_deviation(values):
        return np.std(values)


class TwitterListener(StreamListener):
    def __init__(self, num_tweets_to_grab, stat_analyzer, window=1):
        self.counter = 0
        self.iteration = 0
        self.num_tweets_to_grab = num_tweets_to_grab
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.twtdb
        self.stat_analyzer = stat_analyzer

        self.start_time = int(round(time.time()))
        self.window = window
        self.htags = {}

    def on_data(self, data):
        try:
            data_json = json.loads(data)

            # We only want to store tweets in English (as per requirement)
            if "lang" in data_json and data_json["lang"] == "en":
                self.db.tweets.insert(data_json)
                twit_tags = TwitterListener.extract_hashtags(datajson=data_json)
                for tag in twit_tags:
                    if self.htags.get(tag['text']):
                        self.htags[tag['text']] += 1
                    else:
                        self.htags[tag['text']] = 1

            self.counter += 1

            if self.counter == self.num_tweets_to_grab or (int(round(time.time())) > self.start_time + self.window):
                self._reset_to_new_frame()

            if self.db.tweets.count() > MAX_COUNT:
                print("Too many tweets persisted")
                return False
        except:
            # TODO: come back to this
            pass

    def _reset_to_new_frame(self):
        try:
            self.db.tweet_stats.insert({'hashtags': self.htags,
                                        'iteration': self.iteration})
            self.iteration += 1
        except:
            print("!!!!!!!!!!!!!!!!!")
        # Perform stat analysis
        print(self.htags)
        # If anomaly is detected provide detailed logs

        # reset to new time/counter frame
        self.start_time = int(round(time.time()))
        self.counter = 0
        self.htags = {}

    def on_error(self, status):
        print(status)

    @staticmethod
    def extract_hashtags(datajson):
        return datajson['entities']['hashtags']


if __name__ == "__main__":
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitter_api = tweepy.API(auth)

    analyzer = StatAnalyzer()

    listener = TwitterListener(num_tweets_to_grab=50, stat_analyzer=analyzer)
    twitter_stream = Stream(auth, listener=listener)

    try:
        twitter_stream.filter(track='trump')
    except Exception as e:
        print(e.__doc__)
