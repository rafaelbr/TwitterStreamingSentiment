import requests
import requests_oauthlib
import json
from kafka import KafkaProducer
import sys

ACCESS_TOKEN = 'TWITTER ACCESS TOKEN'
ACCESS_SECRET = 'TWITTER ACCESS SECRET'
CONSUMER_KEY = 'TWITTER CONSUMER KEY'
CONSUMER_SECRET = 'TWITTER CONSUMER SECRET'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('track', 'trump')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    print(query_url)
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(response)
    return response

def connect_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0,10))
    except Exception as ex:
        print('Error while connecting with Kafka')
        print(str(ex))
    finally:
        return producer

def send_to_kafka(producer, topic, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer.send(topic, key=key_bytes, value=value_bytes)
        producer.flush()
    except Exception as ex:
        print('Error while publising message')
        print(str(ex))

def process_tweets(response):
    producer = connect_kafka_producer()
    for line in response.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print("------------------------------------------")
            send_to_kafka(producer, 'twitter', 'tweet', json.dumps(full_tweet))
        except:
            print(line.decode('utf-8'))

resp = get_tweets()
process_tweets(resp)



