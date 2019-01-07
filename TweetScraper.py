"""
@file_name : TweetScraper.py
@author : Srihari Seshadri
@description : This file periodically scrapes tweets and sends them to the
data hub
@date : 12-03-2018
"""

import json
import pandas as pd
import tweepy
import time
from Messenger import Messenger

EXCHANGE = "twitter_feed"
PUB_TOPIC = "tweet"
DATA_HUB_HOST = ''
DATA_HUB_UNAME = 'admin'
DATA_HUB_PWD = 'password'

TIME_WINDOW = 60*30    # seconds
RETRY_TIME = 60*15  # seconds
DELAY_PUB = 0.5   # seconds

QUERY_STR = '"test_tweet_because_why_not"'
# QUERY_STR = '"university of chicago" -filter:retweets'


def get_tweets(twitter_api, query):
    output = []
    for tweet in tweepy.Cursor(twitter_api.search, q=query, lang='en').items():
        temp = tweet._json
        temp['keywords'] = query.strip('"')
        output.append(temp)
    return output


def scraper():
    # Credentials
    consumer_key = '#######'
    consumer_secret = '#######'
    access_key = '#######'
    access_secret = '#######'

    # Set authentication factors
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)

    # Find the query topics
    twitter_api = tweepy.API(auth)
    tweet_list = get_tweets(twitter_api, QUERY_STR)

    # Send the tweets as a message
    messenger = Messenger()
    # Connect to the data hub
    messenger.connect(host=DATA_HUB_HOST,
                      uname=DATA_HUB_UNAME,
                      pwd=DATA_HUB_PWD)

    # Connect to the exchange
    messenger.connect_to_exchange(ex_name=EXCHANGE)

    # Send update
    for tweet in tweet_list:
        message = json.dumps(tweet)
        messenger.send_message_to_exchange(ex_name=EXCHANGE,
                                           message=message,
                                           topic=PUB_TOPIC)
        print("Sent message : ")
        for k, v in tweet.items():
            print(k, "-->", v)

        print("on topic :", PUB_TOPIC)
        time.sleep(DELAY_PUB)


def main():

    print(" Beginning Scraper ")

    # Schedule the scraper to run every 30 minutes
    while True:
        try:
            scraper()
        except Exception as e:
            print("An error occured while scraping tweets : ")
            print(e)
            print("\nRetrying in 15 minutes...")
            time.sleep(RETRY_TIME)
            continue
        time.sleep(TIME_WINDOW)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(" Exiting Scraper. Bye! ")
