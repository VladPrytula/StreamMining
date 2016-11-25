import signal
from datetime import datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream, API
from config_api import *
from pymongo import MongoClient
from processing_utils import StatAnalyzer, TweetProcessor
import time
import json
import logging
import logging.config
import os
import sys
import collections


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
            logger.error('cannot connect to db')
    else:
        logger.error('not implemented')

    return db, client


def exit_gracefully(signal, frame):
    logger.warn("Shutdown signal received! Shutting down.")
    sys.exit(0)


signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)


class TwitterListener(StreamListener):
    def __init__(self, db, client, num_tweets_to_grab, stat_analyzer, window=1, logger=None, processor=None):
        self.logger = logger or logging.getLogger('listener' + __name__)
        self.processor = processor or TweetProcessor()
        print(self.processor)

        if not db or not client:
            self.logger.info('falling back to defaulted mongo connection')
            self.client = MongoClient('localhost', 27017)
            self.db = self.client.twtdb
        else:
            self.db = db
            self.client = client
            self.logger.info("db and client passed in")

        self.counter = 0
        self.iteration = 0
        self.num_tweets_to_grab = num_tweets_to_grab
        self.stat_analyzer = stat_analyzer
        self.start_time = int(round(time.time()))
        self.window = window
        self.htags = collections.defaultdict(int)

    def on_data(self, data):
        try:
            # we want to failfast if limit is reached
            if self.db.tweets.count() > MAX_COUNT:
                self.logger.info("Too many tweets persisted")
                return False

            data_json = json.loads(data)

            # We only want to store tweets in English (as per requirement)
            if self.processor.check_language(data_json):
                self._build_local_tag_distribution(data_json)
                self._persist_tweet_data(data_json, self.htags)
                self.counter += 1

            if self.counter == self.num_tweets_to_grab or (int(round(time.time())) > self.start_time + self.window):
                self._persist_frame_data()
                self._reset_to_new_frame()
        except:
            # TODO: come back to this
            logger.error("data processing error")

    def _build_local_tag_distribution(self, data_json):
        for tag in self.processor.extract_hashtags(data_json):
            self.htags[tag['text']] += 1

    def _reset_to_new_frame(self):
        logger.info(self.counter)
        self.iteration += 1
        self.start_time = int(round(time.time()))
        self.counter = 0
        self.htags.clear()

    def on_error(self, status):
        self.logger.error(status)

    def _persist_tweet_data(self, data_json, local_htag_distribution):
        try:
            self.db.tweets.insert(data_json)
            self._update_global_tags_distribution(local_htag_distribution)
        except:
            self.logger.info('was not able to persist to db')

    def _persist_frame_data(self):
        try:
            self.db.tweet_stats.insert({'hashtags': dict(self.htags),
                                        'iteration': self.iteration,
                                        'stamp': datetime.fromtimestamp(self.start_time)})
            self.db.tweets.insert({"iteration": self.iteration, "counter": self.counter})
        except:
            self.logger.info('was not able to persist to db')

    def _update_global_tags_distribution(self, local_htag_distribution):
        if local_htag_distribution:
            try:
                self.db.tweet_stats.update({"global_tags": 1}, {"$inc": dict(local_htag_distribution)})
            except:
                self.logger.error("unable to update global tags dictionary")


if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitter_api = API(auth)

    analyzer = StatAnalyzer()
    twt_db, twt_client = setup_db_connection()

    listener = TwitterListener(num_tweets_to_grab=1000, db=twt_db,
                               client=twt_client, stat_analyzer=analyzer,
                               window=2, logger=logger)
    twitter_stream = Stream(auth, listener=listener)

    try:
        twitter_stream.filter(track='trump')
    except Exception as e:
        logger.error(e.__doc__)


