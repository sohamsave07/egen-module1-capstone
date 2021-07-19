from time import sleep
import logging
import os
import requests
import json
import datetime
import time

from decouple import config
from concurrent import futures

from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future

api_key = config('API_KEY')

pairs = "EUR/USD,USD/EUR,AUD/USD"

url = "https://api.1forge.com/quotes?pairs=" + pairs + "&api_key=" + api_key

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/Soham/Downloads/golden-union-319717-45274a111df9.json"

class DataToPubSub:
    def __init__(self):
        self.project_ID = "golden-union-319717"
        self.topic_ID = "egen-module1-capstone"
        self.publisherclient = PublisherClient()
        self.topic_path = self.publisherclient.topic_path(self.project_ID, self.topic_ID)
        self.publish_futures = []


    def get_currency_values(self):
        """
            Returns currency pair values
            :return: currency pair values.
            Example EURUSD 1.1239 (euro to dollar conversion)
        """
        
        response = requests.get(url)

        data = response.text
        #print(data)
        currencies = json.loads(data)
        #print(currencies)
        forex_currencies = ""

        for currency in currencies:
            for key, value in currency.items():
                if key == "s":
                    symbol_from = value[:3]
                    symbol_to = value[4:]
                    str_currency = symbol_from + " TO " + symbol_to + ": "
                if key == "p":
                    price = value
            forex_currencies += str_currency  + str(price) + " "

        return forex_currencies

    
    def get_callback(self, publish_future: Future, data: str):
        def callback(publish_future):
            try:
                logging.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logging.error(f"Publishing {data} timed out.")

        return callback


    def publish_data_to_topic(self, data):
        publish_future = self.publisherclient.publish(self.topic_path, data.encode('utf-8'))

        publish_future.add_done_callback(self.get_callback(publish_future, data))
        self.publish_futures.append(publish_future)

        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)

        logging.info(f"Published messages with errorhandler to {self.topic_path}")


def main():

    obj = DataToPubSub()  
    for i in range(25):
        symbol_pairs = obj.get_currency_values()
        obj.publish_data_to_topic(symbol_pairs)
        print(symbol_pairs)      
        time.sleep(5)

if __name__ == '__main__':    
    main()