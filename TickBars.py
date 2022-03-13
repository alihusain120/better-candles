
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
log.basicConfig(level=log.INFO)
from threading import Thread

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

    def __init__(self, tick_threshold: int=1000, to_csv: bool=False):

        with open(os.path.join(sys.path[0], "ticks.txt"), "r") as f:
            self.SYMBOLS = [line.strip() for line in f]

        self.twm = ThreadedWebsocketManager(api_key=APIKEY, api_secret=APISECRET)
        self.streams = [f"{s.lower()}@trade" for s in self.SYMBOLS]
        self.stream_error = False

        self.TICK_THRESHOLD = tick_threshold # Candle for every {TICK_THRESHOLD} trades that occur on Binance for symbol

        self.running_trades = {}
        self.candles = {}
        for s in self.SYMBOLS:
            self.running_trades[s] = []
            self.candles[s] = []


        self.TO_CSV = to_csv
        if to_csv:
            self.CSV_PATHS = {}
            for s in self.SYMBOLS:
                self.CSV_PATHS[s] = f'TickBars_{s}.csv'


    def add_candle_to_csv(self, candle, ticker):

        log.info(f"Adding candle to csv: {candle}")

        if os.path.exists(self.CSV_PATHS[ticker]):
            with open(self.CSV_PATHS[ticker], "a+", newline='') as write_obj:
                csv_writer = csv.DictWriter(write_obj, candle.__dict__.keys())
                csv_writer.writerow(candle.__dict__)
            log.info(f"Added candle to existing csv.")

        else:
            # Write header for file creation
            with open(self.CSV_PATHS[ticker], "a+", newline='') as write_obj:
                csv_writer = csv.DictWriter(write_obj, candle.__dict__.keys())
                csv_writer.writeheader()
                csv_writer.writerow(candle.__dict__)
            log.info(f"Added candle to new csv.")

    
    def create_candle(self, trades):
        current_ticker = trades[0]['s']
        log.info(f"Creating candle for {current_ticker}")

        prices = [float(x['p']) for x in trades]
        vol = sum([float(x['q']) for x in trades])
        candle = self.Candle(prices[0], max(prices), min(prices), prices[-1], vol, trades[0]['T'], trades[-1]['T'])
        
        if self.TO_CSV:
            self.add_candle_to_csv(candle, current_ticker) 

        # Feed candles to other scripts or trade logic from here
        self.candles[current_ticker].append(candle) 

    '''
    {'stream': 'btcusdt@trade', 'data': {'e': 'trade', 'E': 1645736675086, 's': 'BTCUSDT', 't': 1268815872, 'p': '38400.00000000', 'q': '0.07257000', 'b': 9534389397, 'a': 9534372700, 'T': 1645736675085, 'm': False, 'M': True}}
    '''
    
    def handle_message(self, msg):
        if 'data' in msg:
            current_ticker = msg['data']['s']
          
            # assurance
            assert(current_ticker in self.SYMBOLS)

            self.running_trades[current_ticker].append(msg['data'])

            if len(self.running_trades[current_ticker]) >= self.TICK_THRESHOLD:
                self.create_candle(self.running_trades[current_ticker])
                self.running_trades[current_ticker].clear()

        else:
            print(f"Error processing, message: {msg}. Restarting stream.")
            self.stream_error = True

    def stream(self):
        print("Beginning Binance data stream. Ctrl-c to quit.")
        self.twm.start()
        
        self.multiplex = self.twm.start_multiplex_socket(callback=self.handle_message, streams=self.streams)

        stop_trades = Thread(target = self.restart_stream, daemon = True)
        stop_trades.start()

    def restart_stream(self):
        while True:
            time.sleep(1)
            if self.stream_error == True:
                self.twm.stop_socket(self.multiplex)
                time.sleep(5)
                self.stream_error = False
                self.multiplex = self.twm.start_multiplex_socket(callback = self.handle_message, streams = self.streams)

    def stop_stream(self):
        self.twm.stop()
    

def main():
    tb = TickBars(to_csv=True)
    tb.stream()
    
# python TickBars.py <ticker OR .txt path> <tick interval> <-v> <-c>

if __name__ == "__main__":
    main()

