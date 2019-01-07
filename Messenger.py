"""
@file_name : Messenger.py
@author : Srihari Seshadri
@description : This file does the following operations :
                1. Connects to a data hub
                2. Sends messages to exchange/queue based on configuration
@date : 11-14-2018
"""

import pika
import pandas as pd


class Messenger:

    def __init__(self):
        self._connection = None
        self._channel = None
        pass

    def connect(self, host='localhost', port=5672,
                uname="guest", pwd="guest"):
        """
        Establishes a connection and opens a channel for communication
        :param host: IP address of the data hub
        :param port: Port of communication (AMQP is 5672)
        :param uname: Username for authentication
        :param pwd: password for user
        :return: 1 if success, -1 if failure
        """
        try:
            # Make a credentials object
            credentials = pika.PlainCredentials(uname, pwd)
            # First establish the connection
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,
                    port=port,
                    credentials=credentials
                ))
            self._channel = self._connection.channel()
        except Exception as e:
            print("Exception caught while trying to establish connection :")
            print(e)
            return -1
        return 1

    def connect_to_exchange(self, ex_name, ex_type='topic'):
        """
        Checks if there's an exchange, and if not, creates it.
        :param ex_name: Name of the exachange
        :param ex_type: Type of the exchange
        :return: 1 if success, -1 if failure
        """
        try:
            self._channel.exchange_declare(exchange=ex_name,
                                           exchange_type=ex_type)
        except Exception as e:
            print("Exception caught while trying to connect to exchange :")
            print(e)
            return -1
        return 1

    def send_message_to_exchange(self, ex_name="", message="", topic=""):
        """
        Sends a message to the exchange with a specific topic
        :param ex_name: Name of the exchange to send to (NOTE: exchange must
                        already exist for this to be a success)
        :param message: String object containing the message
        :param topic: Topic of the message
        :return: 1 if success, -1 if failure
        """

        # Use the topic to see if we are sending a dataframe
        try:
            if "__df" in topic:
                self._channel.basic_publish(exchange=ex_name,
                                            routing_key=topic,
                                            body=message.to_string())
            else:
                self._channel.basic_publish(exchange=ex_name,
                                            routing_key=topic,
                                            body=message)
        except Exception as e:
            print("Exception caught while trying to send message :")
            print(e)
            return -1
        return 1

    def disconnect(self):
        self._connection.close()

    def __del__(self):
        self._connection.close()



def main():
    pass


if __name__ == "__main__":
    main()
