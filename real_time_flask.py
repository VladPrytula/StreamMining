import json

from flask import Flask, render_template, make_response
from flask import request
from pymongo import MongoClient

app = Flask(__name__)


def _connect_mongo(host, port, username, password, db):
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)
    return conn[db]


def _read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    cursor = db[collection].find(query)
    return list(cursor)


@app.route('/')
def hello_world():
    return render_template('index.html', data='test')


@app.route('/live-iteration')
def tweet_frequency():
    """Creates a live response from frequency buckets
    """
    iteration = request.args.get('iteration', '')
    search_result = _read_mongo('twtdb', 'tweets', {"iteration": int(iteration)})
    if len(search_result):
        data = [iteration, search_result[0].get('counter', 0)]
        response = make_response(json.dumps(data))
        response.content_type = 'application/json'
    else:
        data = [iteration, 0]
        response = make_response(json.dumps(data))
        response.content_type = 'application/json'
    return response


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
