### Install instructions: ### 
* Install mongoDB locally.
* While remote mongodb connections and custom ports are supported through the arguments (see below), for sake of simplicity, please run mongod on default port, which is 27017 and locally. For mongodb installation instructions please refer to https://docs.mongodb.com/v3.2/installation/
* Please do not set up a password, since it is not currently supported
* Create python virtual environment with Python 3.5 or above
* Activate virtualenv
* Install required dependencies using requirements.txt that can be find in the root
* pip3 install -r requirements.txt

### Approach: ###
1. Utility connects to Twitter using Streaming API via Tweepy
2. Streaming data is received in chunks bounded by:
 1. either time frame.
 2. or by the number of tweets that fit into the frame. 
3. Stores streamed data in mongoDB for future off-line analysis
4. Computes running statistics for each time frame
5. Global statistics for mean and deviations of tag frequency is updated
after each time frame
6. Anomaly detection is rudimentary at this time:
 1. if mean of tags frequency distribution is within approx 95% confidence interval for all frames it is considered no-anomaly case.
 2. if means is out of the range the frame is marked as the one that contains anomaly and frame is logged.
 3. not_implemented: one simple addition would be use globally accumulated tags dictionary to exclude tags tha appear with high mass but not in every frame
    
    
### Manual: ###

_Console utility:_
* run streamer.py (MongoDB server must be running)
* --w. This defines a list of key words that are used for stream filtering.
** Example: python streamer.py --w python mongodb
* --l. Define a location that is used for stream filtering. Defaulted to Germany = [5.0770049095, 47.2982950435, 15.0403900146, 54.9039819757]
** Example: python streamer.py --l 1 1 2 2
* --dbport. Used to define a port number where mongoDB is running
** Example: python streamer.py --dbport 27017
* --dbhost. Used to define a host number where mongoDB is running
** Example: python streamer.py --dbport localhost  
* --twt_per_frame. Limits the max amount of tweets that can be recieved per frame
** Example: python --twt_per_frame 500
* --flength. Lenght of window frame in seconds.
** Example: python --flength 3


_Front end:_ 

* start real_time_flask.py (MondoDB server must be running). This will start tiny flask server with two active endpoints:
** Live frequency data: /live-iteration, shows dymaic plot of number ot tweets per frame
** Latest anomaly: /latest-anomaly, shows unformatted frequency distribution of tags for the last frame that was detected as anomalous
