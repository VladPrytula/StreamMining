import signal
from tweepy import OAuthHandler, Stream, API
from TweetListener import TwitterListener
from config_api import *
from processing_utils import StatAnalyzer, TweetPersistor
import json
import logging
import logging.config
import os
import sys
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

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitter_api = API(auth)
    parser.add_argument('--w', dest='words', nargs='*',
                        default='python',
                        help='Define a list of key words that are used for stream filtering: --w word1 word2',
                        )
    parser.add_argument('--l', dest='latlong', nargs='*',
                        default=[5.0770049095, 47.2982950435, 15.0403900146, 54.9039819757], type=int,
                        help='Define a location that is used for stream filtering: --l lat1 long1 lat2 long2',
                        )
    parser.add_argument('--dbport', dest='dbport',
                        default=27017, type=int,
                        help='Define a port number where mongoDB is running: --dbport 27017',
                        )
    parser.add_argument('--dbhost', dest='dbhost',
                        default='localhost', type=str,
                        help='Define a host where mongoDb is running: --dbhost localhost',
                        )
    parser.add_argument('--twt_per_frame', dest='twt_per_frame',
                        default=500, type=int,
                        help='max umber of tweets to grab per frame: --twt_per_frame 100',
                        )
    parser.add_argument('--flength', dest='window',
                        default=3, type=int,
                        help='time frame length in seconds: --flength 3',
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
    dbhost = args.dbhost
    dbport = args.dbport
    frame_window = args.window
    tweets_to_grab = args.twt_per_frame

    twt_persistor = TweetPersistor(host=dbhost, port=dbport)
    twt_analyzer = StatAnalyzer(twt_peristor=twt_persistor)

    twt_listener = TwitterListener(num_tweets_to_grab=tweets_to_grab,
                                   persistor=twt_persistor,
                                   stat_analyzer=twt_analyzer,
                                   window=frame_window, max_count=MAX_COUNT,
                                   logger=logger)
    twitter_stream = Stream(auth, listener=twt_listener)

    try:
        twitter_stream.filter(track=stop_words, locations=geo_box)
    except Exception as e:
        logger.error(e.__doc__)
