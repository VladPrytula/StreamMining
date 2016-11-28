from processing_utils import StatAnalyzer, TweetProcessor, TweetPersistor
from datetime import datetime
from tweepy.streaming import StreamListener
import time
import collections
import logging
import json


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
        """When end of frame is reached (either due to max number of tweets per frame or time limit frame is analyzed
        for presence of anomalies. If anomaly is detected it is persisted to db, written to anomaly.txt log file.
        Global statistics is updated by taking into account local mean and std for the current frame.
        New frame starts.
        """
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
        """ Persists local frame statistics such as
            1. timestammp when frame started
            2. current frame number
            3. frequency of tag occurrences per frame
            4. total number of tweets per current frame

            One must note that item 3 (frequency of tag occurrences) is somewhat time-frame specific information
            that is not easily extended to global statistics due to the fact that different tags appear in different
            frames. On the other hand, running experiments for not "long" time-frames shows that frequency distribution
            per frame is rather stable from frame to frame.
        """
        self.logger.info(dict(self.htags))
        self.persistor.insert_statistics({'local_tags': dict(self.htags),
                                          'iteration': self.iteration,
                                          'stamp': datetime.fromtimestamp(self.start_time),
                                          'local_tags_count': len(self.htags),
                                          'local_tweets_count': self.counter,
                                          'local_tags_frequency_mean':
                                              StatAnalyzer.compute_first_moment(list(self.htags.values())),
                                          'local_tags_frequency_std':
                                              StatAnalyzer.compute_second_moment(list(self.htags.values()))})
