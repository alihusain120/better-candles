
'''
Streams trades from Binance API, aggregates and delivers as Tick bars (configurable)
    
    Lopez de Prado, Marcos. 2018. Advances in Financial Machine Learning. New York, NY: John Wiley & Sons:
        2.3.1.2 Tick Bars
            The idea behind tick bars is straightforward: The sample variables listed earlier
            (timestamp, VWAP, open price, etc.) will be extracted each time a pre-defined number 
            of transactions takes place, e.g., 1,000 ticks. This allows us to synchronize sampling
            with a proxy of information arrival (the speed at which ticks are originated).
    
'''

import time
import os, sys

from datetime import datetime
from binance import ThreadedWebsocketManager
import csv
import config
import logging as log
#log.basicConfig(level=log.INFO)

APIKEY = config.APIKEY
APISECRET = config.SECRET

class TickBars:

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

    def __init__(self, symbol: str='BTCUSDT', tick_threshold: int=1000, to_csv: bool=False, csv_filepath: str=''):
        # TODO: Add support for str[] of symbols in place of symbol parameter

        # self.last_time = time.time()

        self.twm = ThreadedWebsocketManager(api_key=APIKEY, api_secret=APISECRET)

        self.SYMBOL = symbol
        self.TICK_THRESHOLD = tick_threshold # Candle for every {TICK_THRESHOLD} trades that occur on Binance for symbol

        self.running_trades = []
        self.candles = []

        self.TO_CSV = to_csv
        self.CSV_PATH = None
        if to_csv:
            if csv_filepath.endswith('.csv'):
                self.CSV_PATH = csv_filepath
            else:
                # TODO: Edit for multiple symbol streaming functionality
                self.CSV_PATH = f'TickBars_{symbol}.csv'


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

        # Feed candles to other scripts or trade logic from here
        self.candles.append(candle) 

    
    def handle_message(self, msg):
        self.running_trades.append(msg)

        if len(self.running_trades) >= self.TICK_THRESHOLD:
            self.create_candle(self.running_trades)
            self.running_trades.clear()

    def stream(self):
        print("Beginning Binance data stream. Ctrl-c to quit.")
        self.twm.start()
        self.twm.start_trade_socket(callback=self.handle_message, symbol=self.SYMBOL)

    def stop_stream(self):
        self.twm.stop()
    

def main():
    tb = TickBars(to_csv=True)
    tb.stream()
    
# python TickBars.py <ticker OR .txt path> <tick interval> <-v> <-c>

if __name__ == "__main__":
    main()
