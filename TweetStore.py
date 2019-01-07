"""
@file_name : TweetCleaner.py
@author : Srihari Seshadri
@description : This file receives the tweets from the datahub
                and stores them into a database
@date : 12-03-2018
"""


import os
import json
import time
import pandas as pd
from Receiver import Receiver
from SQLDatabaseManager import SQLDatabaseManager

EXCHANGE = "twitter_feed"
SUB_TOPIC = "cleaned"
DATA_HUB_HOST = ''
DATA_HUB_UNAME = 'admin'
DATA_HUB_PWD = 'password'

# DB_IP = 'localhost'
DB_IP = ''
DATABASE = 'tweets'
DB_UNAME = 'rtis'
DB_PWD = 'rtis'


def get_df(tweet):
    keys = ['id_str',
            'created_at_epoch', 'scraped_at_epoch',
            't', 'retweet_count', 'favorite_count', 'keywords']
    tempdict = {}
    for key in keys:
        tempdict[key] = tweet.get(key)
    return tempdict


def callback(ch, method, properties, body):
    """
    This callback takes the data and stored it into the database
    :return: 1 if success, -1 if fail
    """
    try:
        content_str = body.decode("utf-8")

        json_msg = json.loads(content_str)

        # Push the message into the database
        json_df = get_df(json_msg)
        uc_df = pd.DataFrame(json_df, index=[0])

        sqldbm = SQLDatabaseManager()

        port = '3306'
        ret = sqldbm.connect(host=DB_IP,
                             database=DATABASE,
                             username=DB_UNAME,
                             password=DB_PWD,
                             port=port)

        if ret != 1:
            print(" Closing program ")
            return

        sqldbm.insert(dframe=uc_df, table_name="UCHICAGO",
                      if_table_exists="append")

        print("Message received -> stored")
        return 1
    except Exception as e:
        print("Exception caught in callback ")
        print(e)
        print("Message could not be saved to DB. Skipping ....")
        return -1


def main():

    # Receive all the tweets and push them into the database
    print("Waiting for tweets")

    receiver = Receiver()

    # Connect to the data hub
    receiver.connect(host=DATA_HUB_HOST,
                     uname=DATA_HUB_UNAME,
                     pwd=DATA_HUB_PWD)

    # Connect to the exchange
    try:
        ret = receiver.get_data_from_exchange(ex_name=EXCHANGE,
                                              topic=SUB_TOPIC,
                                              callback=callback)
    except Exception as e:
        print(" An exception was caught while getting data from the exchange :")
        print(e)

        print("\n Exiting .... \n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(" Exiting Store. Bye! ")
