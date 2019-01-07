"""
@file_name : TweetCleaner.py
@author : Srihari Seshadri
@description : This file does the following operations :
                1. Connects to a data hub
                2. Receives messages from exchange/queue based on config
                3. Calls appropriate pipelines based on decoded message
@date : 12-03-2018
"""

import json
import time
from email.utils import parsedate_tz, mktime_tz
from Receiver import Receiver
from Messenger import Messenger

EXCHANGE = "twitter_feed"
SUB_TOPIC = "tweet"
PUB_TOPIC = "cleaned"
DELAY_PUB = 1   # seconds

DATA_HUB_HOST = ''
DATA_HUB_UNAME = 'admin'
DATA_HUB_PWD = 'password'


def to_epoch(tweet_time_string):
    """
    Convert rfc 5322 -like time string into epoch
    """
    return mktime_tz(parsedate_tz(tweet_time_string))


def clean_message(json_msg):
    json_msg['created_at_epoch'] = to_epoch(json_msg['created_at'])
    json_msg['scraped_at_epoch'] = int(time.time())
    json_msg['t'] = int(json_msg['scraped_at_epoch'] -
                        json_msg['created_at_epoch'] / 1800)
    return json_msg


def callback(ch, method, properties, body):
    """
    Callback function for cleaning tweets
    :return: the cleaned json message
    """
    try:
        content_str = body.decode("utf-8")

        json_msg = json.loads(content_str)
        # print("Received : ")
        # for k, v in json_msg.items():
        #     print(k, "-->", v)

        clean_jmsg = clean_message(json_msg)

        # Push the cleaned message to the broker again
        messenger = Messenger()
        messenger.connect(host=DATA_HUB_HOST,
                          uname=DATA_HUB_UNAME,
                          pwd=DATA_HUB_PWD)

        # Connect to the exchange
        message = json.dumps(clean_jmsg)
        messenger.connect_to_exchange(ex_name=EXCHANGE)
        messenger.send_message_to_exchange(ex_name=EXCHANGE,
                                           message=message,
                                           topic=PUB_TOPIC)
        time.sleep(DELAY_PUB)

        print("Message received -> cleaned -> forwarded")

    except Exception as e:
        print("Exception caught in callback ")
        print(e)
        print("Message could not be saved to DB. Skipping ....")
        return -1
    return 1


def main():

    # Receive all messages, process them, and forward them to the broker
    print("Waiting for tweets")
    receiver = Receiver()

    # Connect to the data hub
    receiver.connect(host=DATA_HUB_HOST,
                     uname=DATA_HUB_UNAME,
                     pwd=DATA_HUB_PWD)

    # Connect to the exchange
    try:
        print(" Subscribing to topic", SUB_TOPIC)
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
        print(" Exiting Cleaner. Bye! ")
