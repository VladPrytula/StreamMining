from pymongo import MongoClient
import time

if __name__ == "__main__":
    try:
        client = MongoClient('localhost', 27017)
        db = client.twtdb
        statistics = db.tweet_stats
        statistics.insert({"tags": {'key1':1, 'key2':2}, "timestamp": time.time(), "frame":0})
    except:
        print("failed to setup required collections")