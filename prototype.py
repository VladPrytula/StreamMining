import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from config_api import *
import json
from pymongo import MongoClient
import time
import numpy as np


class TwitterListener(StreamListener):
    """
    We need to add some sort of a counter to make it stop.
    You can’t add this to the on_data() function, as it is called fresh each time.
    But we can add it to the class during initialization, and so it will be available each time on_data()
    is called.
    """

    def __init__(self, num_tweets_to_grab, hashtags, iteration, window=1):
        self.counter = 0
        self.num_tweets_to_grab = num_tweets_to_grab
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.twtdb
        self.iteration = iteration

        self.start_time = int(round(time.time()))
        self.window = window
        self.htags = hashtags

    def on_data(self, data):
        """
        The problem was that the live Twitter stream in wild and unpredictable.
        Normally, when you search for Tweets, you always get the same json object
        You get deleted tweets (why?!), tweets in strange formats. 80-90% of the time,
        the tweets are as you expect.
        We need to cope with the remaining corner cases. Here is our first attempt to fix it:
        :param data:
        :return:
        """
        try:
            datajson = json.loads(data)
            """This will be called each time we receive stream data"""
            # We only want to store tweets in English (as per requirement)
            if "lang" in datajson and datajson["lang"] == "en":
                # Store tweet info into the cooltweets collection.
                self.db.tweets.insert(datajson)
                twit_tags = self.extract_hashtags(datajson=datajson)
                for tag in twit_tags:
                    if self.htags.get(tag['text']):
                        self.htags[tag['text']] += 1
                    else:
                        self.htags[tag['text']] = 1

            self.counter += 1

            if self.counter == self.num_tweets_to_grab or (int(round(time.time())) > self.start_time + self.window):
                # We return False, which causes the class to exit (this is how Tweepy works internally.
                # If you return True, it keeps looking for new tweets).
                print("tweets per frame " + str(self.counter))
                self.db.tweets.insert({"iteration": self.iteration, "counter": self.counter})
                return False
        except:
            # TODO: come back to this
            pass

    def on_error(self, status):
        print(status)

    @staticmethod
    def extract_hashtags(datajson):
        return datajson['entities']['hashtags']


def compute_average(values):
    return np.mean(values)


def compute_deviation(values):
    return np.std(values)


if __name__ == "__main__":
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitter_api = tweepy.API(auth)

    client = MongoClient('localhost', 27017)
    db = client.twtdb

    running_mean = 0
    running_std = 0
    counter = 0
    hashtags = {}
    listener = TwitterListener(num_tweets_to_grab=100, hashtags=hashtags, iteration=counter)
    twitter_stream = Stream(auth, listener=listener)

    while db.tweets.count() < MAX_COUNT:

        hashtags = {}
        listener.iteration = counter
        listener.htags = hashtags

        try:
            twitter_stream.filter(track='trump')
            print(listener.htags)
            print(len(listener.htags))
        except Exception as e:
            print(e.__doc__)
            #
            # if running_mean - 2 * running_std <= compute_average(hashtags.values()) <= running_mean - 2 * running_std:
            #     pass # print("Anomaly")
        counter += 1
