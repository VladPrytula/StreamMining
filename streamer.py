from datetime import datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream, API
from config_api import *
from pymongo import MongoClient
import time
import numpy as np
import json
import logging
import logging.config
import os


def setup_logging(default_path='logging.json', default_level=logging.INFO, env_key='LOG_CFG'):
    """Setup logging configuration
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def setup_db_connection(dbtype='mongo', port=27017, host='localhost'):
    if dbtype == 'mongo':
        try:
            client = MongoClient(host, port)
            db = client.twtdb
        except:
            pass
            logger.error('cannot connect to db')
    else:
        print('not implemented')

    return db, client


class StatAnalyzer:
    @staticmethod
    def compute_average(values):
        return np.mean(values)

    @staticmethod
    def compute_deviation(values):
        return np.std(values)


class TwitterListener(StreamListener):
    def __init__(self, db, client, num_tweets_to_grab, stat_analyzer, window=1, logger=None):
        self.logger = logger or logging.getLogger('listener' + __name__)

        if not db or not client:
            self.logger.info('falling back to defaulted mongo connection')
            self.client = MongoClient('localhost', 27017)
            self.db = self.client.twtdb
        else:
            self.client = db
            self.db = client

        self.counter = 0
        self.iteration = 0
        self.num_tweets_to_grab = num_tweets_to_grab
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
                self.logger.info("Too many tweets persisted")
                return False
        except:
            # TODO: come back to this
            pass

    def _reset_to_new_frame(self):
        try:
            self.db.tweet_stats.insert({'hashtags': self.htags,
                                        'iteration': self.iteration,
                                        'stamp': datetime.fromtimestamp(self.start_time)})
            self.db.tweets.insert({"iteration": self.iteration, "counter": self.counter})
            logger.info(self.counter)
            self.iteration += 1
        except:
            self.logger.info('was not able to persist to tweet_stats collection')
        # Perform stat analysis
        # self.logger.info(self.htags)
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
    setup_logging()
    logger = logging.getLogger(__name__)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitter_api = API(auth)

    analyzer = StatAnalyzer()
    db, client = setup_db_connection()

    listener = TwitterListener(num_tweets_to_grab=1000, db=db,
                               client=client, stat_analyzer=analyzer,
                               window=2, logger=logger)
    twitter_stream = Stream(auth, listener=listener)

    try:
        twitter_stream.filter(track='trump')
    except Exception as e:
        logger.error(e.__doc__)
