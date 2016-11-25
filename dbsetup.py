from pymongo import MongoClient

if __name__ == "__main__":
    try:
        client = MongoClient('localhost', 27017)
        db = client.twtdb
        statistics = db.tweet_stats
        statistics.insert({"global_tags": 1})
    except:
        print("failed to setup required collections")