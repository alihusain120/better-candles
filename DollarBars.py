
'''
Streams trades from Binance API, aggregates and delivers as Volume bars (configurable)
    
    Lopez de Prado, Marcos. 2018. Advances in Financial Machine Learning. New York, NY: John Wiley & Sons:
        2.3.1.4 Dollar Bars
            Dollar bars are formed by sampling an observation every time a pre-defined market value is exchanged. 
'''

import time
import os, sys

from datetime import datetime
from binance import ThreadedWebsocketManager
import csv
import config
import logging as log
log.basicConfig(level=log.INFO)

APIKEY = config.APIKEY
APISECRET = config.SECRET

class DollarBars:

    class Candle:
        def __init__(self, o, h, l, c, v, t1, t2):
            self.open = o
            self.high = h
            self.low = l
            self.close = c
            self.volume = v
            self.open_timestamp = t1
            self.close_timestamp = t2
        
        def __str__(self):
            return ",\n".join([f"{x}:{y}" for x, y in self.__dict__.items()])


    def __init__(self, symbol: str='BTCUSDT', dollar_threshold: float=100000.0, to_csv: bool=False, csv_filepath: str=''):
        # TODO: Add support for str[] of symbols in plae of symbol parameter

        self.twm = ThreadedWebsocketManager(api_key=APIKEY, api_secret=APISECRET)
        
        self.SYMBOL = symbol
        self.DOLLAR_THRESHOLD = dollar_threshold # Candle sampled at every {vol_threshold} units traded of the base asset
        
        self.running_trades = []
        self.running_dollars = 0.0
        self.candles = []

        self.TO_CSV = to_csv
        self.CSV_PATH = None
        if to_csv:
            if csv_filepath.endswith('.csv'):
                self.CSV_PATH = csv_filepath
            else:
                self.CSV_PATH = f'VolumeBars_{symbol}.csv'

    def add_candle_to_csv(self, candle):
        log.info(f"Adding candle to csv: {candle}")

        if os.path.exists(self.CSV_PATH):
            with open(self.CSV_PATH, "a+", newline='') as write_obj:
                csv_writer = csv.DictWriter(write_obj, candle.__dict__.keys())
                csv_writer.writerow(candle.__dict__)
            log.info(f"Added candle to existing csv.")

        else:
            # Write header for file creation
            with open(self.CSV_PATH, "a+", newline='') as write_obj:
                csv_writer = csv.DictWriter(write_obj, candle.__dict__.keys())
                csv_writer.writeheader()
                csv_writer.writerow(candle.__dict__)
            log.info(f"Added candle to new csv.")


    def create_candle(self, trades):
        prices = [float(x['p']) for x in trades]
        vol = sum([float(x['q']) for x in trades])
        candle = self.Candle(prices[0], max(prices), min(prices), prices[-1], vol, trades[0]['T'], trades[-1]['T'])
        
        if self.TO_CSV:
            self.add_candle_to_csv(candle)

        self.candles.append(candle)
    
    def handle_message(self, msg):
        self.running_trades.append(msg)
        self.running_dollars += float(msg['q'])*float(msg['p'])

        log.info(f"Trade Ingested at {msg['E']}, p={msg['p']}, q={msg['q']}")
        if len(self.running_trades) % 50 == 0:
            log.info(f"{len(self.running_trades)} total trades")

        if self.running_dollars >= self.DOLLAR_THRESHOLD:
            self.create_candle(self.running_trades)
            self.running_trades.clear()
            self.running_dollars = 0.0

    def stream(self, symbol: str='BTCUSDT' ,to_csv: bool=False, csv_filepath: str=''):

        self.f = None
        if to_csv:
            if csv_filepath.endswith('.csv'):
                f = open(csv_filepath, 'a')
            else:
                # TODO: Edit for multiple symbol streaming functionality
                f = open(f'VolumeBars_{symbol}.csv', 'a')

        twm = ThreadedWebsocketManager(api_key=APIKEY, api_secret=APISECRET)
        twm.start()
        twm.start_trade_socket(callback=self.handle_message, symbol=self.SYMBOL)x
    

if __name__ == "__main__":
    db = DollarBars(to_csv=True)
    db.stream()
