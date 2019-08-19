# TwitterStreamingSentiment

Execute online prediction of sentiment on Twitter Streaming API. The project consist in two parts. First a python application which run the Twitter API and saves data onto Apache Kafka topic. The second part is a Scala application running on top of Apache Spark which reads Kafka streaming and classifies text in real time using a previous saved SparkML model.

## Preparation

* Install SBT tools
* Install and configure Apache Kafka server
* Create Twitter App and get credentials
* Edit python/twitterstream.py with credentials
* Copy a pre-generated SparkML model trained on some Twitter dataset into src/main/resources

You can use the following project to generate the model:

https://github.com/rafaelbr/TwitterSentimentAnalysis

## How to run

Ensure twitterstream.py is running with

$ python python/twitterstreaming.py

Execute the following command:

$ sbt run
