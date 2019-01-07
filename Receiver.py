"""
@file_name : Receiver.py
@author : Srihari Seshadri
@description : This file does the following operations :
                1. Connects to a data hub
                2. Receives messages from exchange/queue based on config
                3. Calls appropriate pipelines based on decoded message
@date : 11-14-2018
"""

import pika
import pandas as pd
from pandas.compat import StringIO


class Receiver:

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

    def get_data_from_exchange(self,
                               queue_name="",
                               ex_name="",
                               ex_type='topic',
                               topic="",
                               callback=None):
        """
        Starts the process for listening in and consuming the queue. The
        callback function acts as the handler for deciding pipelines
        :param queue_name: Name of the queue. Leave empty to auto generate
        :param ex_name: Name of the exchange to conect to
        :param ex_type: Exchange Type
        :param topic: Topic to subscribe to
        :param callback: Callback function to call
        :return: -1 if failure. It should continue running until quit.
        """
        try:
            self._channel.exchange_declare(exchange=ex_name,
                                           exchange_type=ex_type)
            if queue_name == "":
                result = self._channel.queue_declare(exclusive=True)
            else:
                result = self._channel.queue_declare(queue=queue_name,
                                                     exclusive=True)
            queue_name = result.method.queue

            self._channel.queue_bind(exchange=ex_name,
                                     queue=queue_name,
                                     routing_key=topic)

            # Based on topic, we decide what callback has to be used
            if callback is not None:
                self._channel.basic_consume(callback,
                                            queue=queue_name)
            else:
                if "__df" in topic:
                    self._channel.basic_consume(self._dataframe_callback,
                                                queue=queue_name)
                else:
                    self._channel.basic_consume(self._callback,
                                                queue=queue_name)

            self._channel.start_consuming()
        except Exception as e:
            print("An error occured while trying to receive data from queue")
            print(e)
            return -1
        return 1

    @staticmethod
    def _callback(ch, method, properties, body):
        """
        callback function for receiving the information from a queue
        :param ch: channel
        :param method: Method of data travel
        :param properties: properties of message and channel
        :param body: Byte stream of the content in
        :return: None
        """
        try:
            content_str = body.decode("utf-8")

            print("Received : ", content_str)

        except Exception as e:
            print("Exception caught in callback ")
            print(e)
            return -1
        return 1

    @staticmethod
    def _dataframe_callback(ch, method, properties, body):
        """
        callback function for receiving the information from a queue
        :param ch: channel
        :param method: Method of data travel
        :param properties: properties of message and channel
        :param body: Byte stream of the content in
        :return: None
        """
        try:
            content_str = body.decode("utf-8")

            dataframe = pd.read_csv(StringIO(content_str), sep='\s+')

            print("Received Dataframe : ")
            print(dataframe.head())

        except Exception as e:
            print("Exception caught in callback ")
            print(e)
            return -1
        return 1

    def disconnect(self):
        self._connection.close()

    def __del__(self):
        self._connection.close()


def main():
    receiver = Receiver()

    # Connect to the data hub
    data_hub_host = 'localhost'
    receiver.connect(host=data_hub_host)

    # Connect to the exchange
    ex_name = "ping"
    topic = "update"
    receiver.get_data_from_exchange(ex_name=ex_name, topic=topic)


if __name__ == "__main__":
    main()
