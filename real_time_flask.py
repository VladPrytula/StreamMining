import json
from random import random
from time import time

from flask import Flask, render_template, make_response
from flask import request
from pymongo import MongoClient

app = Flask(__name__)


@app.route('/live-iteration')
def new_author():
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


def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]


def _read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and return a collection """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    return list(cursor)


@app.route('/')
def hello_world():
    return render_template('index.html', data='test')


@app.route('/live-data')
def live_data():
    # Create a PHP array and echo it as JSON
    data = [time() * 1000, random() * 100]
    response = make_response(json.dumps(data))
    response.content_type = 'application/json'
    return response

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
