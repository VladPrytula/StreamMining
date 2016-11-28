*Install instructions:*
1. Install mongoDB locally.
    1. While remote mongodb connections and custom ports are supported 
    through the arguments (see below), for sake of simplicity, 
    please run mongod on default port, which is 27017 and locally
    2. for mongodb installation instructions please refer to https://docs.mongodb.com/v3.2/installation/
    3. Please do not set up a password, since it is not currently supported
2. Create python virtual environment with Python 3.5 or above
3. Activate virtualenv
4. Install required dependencies using requirements.txt that can be find in the root
    1. pip3 install -r requirements.txt

*Approach:*
1. Utility connects to Twitter using Streaming API via Tweepy
2. Streaming data is received in chunks bounded by:
    1. either time frame.
    2. or by the number of tweets that fit into the frametotal amount of 
    tweets that can be mined
3. Stores streamed data in mongoDB for off-line analysis if required
4. Computes running statistics for each time frame
5. Global statistics for mean and deviations of tag frequency is updated
after each time frame
6. Anomaly detection is rudimentary at this time:
    1. 
    
    
*Manual:*
_Console utility:_
_Front end:_