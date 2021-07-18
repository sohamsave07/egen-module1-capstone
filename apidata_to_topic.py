"""from requests import Session
from os import environ
from time import sleep
import logging

from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future"""

import requests
import json
import datetime
import time

api_key = "tKFghRTe3mbwlXcvPUYl3NYQeZyL6N1T"

pairs = "EUR/USD,USD/EUR,AUD/USD"

url = "https://api.1forge.com/quotes?pairs=" + pairs + "&api_key=" + api_key

def get_currency_values():
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
    forex_currencies = []

    for currency in currencies:
        for key, value in currency.items():
            if key == "s":
                symbol_from = value[:3]
                symbol_to = value[4:]
                str_currency = symbol_from + " TO " + symbol_to + ": "
            if key == "p":
                price = value
        forex_currencies.append(str_currency  + str(price))

    return forex_currencies


def main():    
    while True:
        symbol_pairs = get_currency_values()
        print(symbol_pairs)      
        time.sleep(5)

if __name__ == '__main__':    
    main()