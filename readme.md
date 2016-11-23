Approach:

1. Connect to Twitter using Streaming API
2. get streaming data in chunks bounded by:
    either time frame or by the number of tweets that fit into the frame
    total amount of tweets that can be mined
    errors on twitter side
3. store streamed data in mongoDB for off-line analysis if required
4. compute running statistics for each time frame
5. ?compute cross time-frame statistics for co-occuring tags?
