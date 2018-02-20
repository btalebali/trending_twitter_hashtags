#!/usr/bin/env python


import socket
import sys
import requests
import requests_oauthlib
import json
import ConfigParser


### read config
config = ConfigParser.ConfigParser()
config.readfp(open(r'config/param.ini'))
ACCESS_TOKEN = config.get('Twitter_app', 'ACCESS_TOKEN')
ACCESS_SECRET = config.get('Twitter_app', 'ACCESS_SECRET')
CONSUMER_KEY = config.get('Twitter_app', 'CONSUMER_KEY')
CONSUMER_SECRET = config.get('Twitter_app', 'CONSUMER_SECRET')




def get_tweets():
    my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True, verify=False)
    return response

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            tweet_text = tweet_text.encode('utf-8')
            tcp_connection.send(tweet_text + '\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
            
            
            
if __name__ == "__main__":
    TCP_IP = "localhost"
    TCP_PORT = 9009
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("Waiting for TCP connection...")
    conn, addr = s.accept()
    print("Connected... Starting getting tweets.")
    resp = get_tweets()
    send_tweets_to_spark(resp, conn)
