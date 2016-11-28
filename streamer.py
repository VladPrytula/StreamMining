import signal
import warnings
from datetime import datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream, API
from config_api import *
from processing_utils import StatAnalyzer, TweetProcessor, TweetPersistor
import time
import json
import logging
import logging.config
import os
import sys
import collections
import argparse

parser = argparse.ArgumentParser()


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


def exit_gracefully(signal, frame):
    logger.warn("Shutdown signal received! Shutting down.")
    sys.exit(0)


signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)


class TwitterListener(StreamListener):
    def __init__(self, persistor,
                 num_tweets_to_grab,
                 stat_analyzer=None,
                 window=1,
                 max_count=5000,
                 logger=None,
                 processor=None):
        self.logger = logger or logging.getLogger('listener' + __name__)
        self.processor = processor or TweetProcessor()
        self.stat_analyzer = stat_analyzer or StatAnalyzer()
        self.persistor = persistor or TweetPersistor()
        self.counter, self.iteration = 0, 0
        self.num_tweets_to_grab = num_tweets_to_grab
        self.start_time = int(round(time.time()))
        self.window = window
        self.max_count = max_count
        self.htags = collections.defaultdict(int)

    def on_data(self, data):
        try:
            # we want to failfast if limit is reached
            if self.persistor.get_tweets_count() > self.max_count:
                self.logger.info("Too many tweets persisted. Aborting stream")
                return False
            self._process_tweet(json.loads(data))
            if self._frame_ended():
                self._process_frame()
        except:
            # TODO: come back to this
            self.logger.error("data processing error %s")

    def on_error(self, status):
        self.logger.error(status)

    def _frame_ended(self) -> bool:
        return self.counter == self.num_tweets_to_grab or (int(round(time.time())) > self.start_time + self.window)

    def _process_tweet(self, data_json):
        if self.processor.check_language(data_json):
            self.processor.process_tweet(data_json, self.htags)
            self.counter += 1

    def _process_frame(self):
        if self.stat_analyzer.detect_local_anomaly(self.htags):
            self.logger.critical("Anomaly detected")
            self.logger.critical(dict(self.htags))
        self.stat_analyzer.detect_global_anomaly(self.htags)
        self._persist_frame_data()
        self._reset_to_new_frame()

    def _reset_to_new_frame(self):
        self.iteration += 1
        self.start_time = int(round(time.time()))
        self.counter = 0
        self.htags.clear()

    def _persist_frame_data(self):
        logger.info(dict(self.htags))
        self.persistor.insert_statistics({'local_tags': dict(self.htags),
                                          'iteration': self.iteration,
                                          'stamp': datetime.fromtimestamp(self.start_time),
                                          'local_tags_count': len(self.htags),
                                          'local_tweets_count': self.counter,
                                          'local_tags_frequency_mean':
                                              StatAnalyzer.compute_first_moment(list(self.htags.values())),
                                          'local_tags_frequency_std':
                                              StatAnalyzer.compute_second_moment(list(self.htags.values()))})


if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitter_api = API(auth)
    parser.add_argument('--w', dest='words', nargs='*',
                        default='python',
                        help='Define a list of key words that are used for stream filtering',
                        )
    parser.add_argument('--l', dest='latlong', nargs='*',
                        default=[5.0770049095, 47.2982950435, 15.0403900146, 54.9039819757], type=int,
                        help='Define a location that is used for stream filtering',
                        )
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()

    stop_words = args.words
    if len(args.latlong) == 4:
        geo_box = args.latlong
    else:
        print(
            """Geo Box must be defined by two point in the form: --l lat1 long1 lat2 long2.
            Falling back to default version of Geo Box [5.0770049095, 47.2982950435, 15.0403900146, 54.9039819757]""")
        geo_box = [5.0770049095, 47.2982950435, 15.0403900146, 54.9039819757]

    twt_persistor = TweetPersistor()
    twt_analyzer = StatAnalyzer(twt_peristor=twt_persistor)

    twt_listener = TwitterListener(num_tweets_to_grab=1000,
                                   persistor=twt_persistor,
                                   stat_analyzer=twt_analyzer,
                                   window=3, max_count=MAX_COUNT,
                                   logger=logger)
    twitter_stream = Stream(auth, listener=twt_listener)

    try:
        twitter_stream.filter(track=stop_words, locations=geo_box)
    except Exception as e:
        logger.error(e.__doc__)
