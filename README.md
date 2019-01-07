# RealTime_InfoTransfer
Simple real time architecture using python for data transfer between servers. Can stream pandas dataframes as well.
The use case in the Python files are tweets scraped via the twitter API
It can easily be transformed into rows of a database, signals, images, stock prices etc.

## Architecture

![alt text](https://github.com/Srihari231092/RealTime_InfoTransfer/blob/master/doc/architecture.png)


##### Data Flow : 
1. Data is scraped from twitter periodically and published to the Broker server under topic “tweets”
    Each tweet in a batch is sent sequentially
2. The analysis server (subscriber) retrieves tweets from the queue of data for cleaning and transformation
3. Analysis server publishes the cleaned tweets to the exchange in the broker under topic “cleaned_tweets”
4. The database subscribes to the exchange for “cleaned_tweets” and stores each tweet for analysis
5. The analysis and visualization module pulls the latest data for tracking and monitoring via a TCP connection


