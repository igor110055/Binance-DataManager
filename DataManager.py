import os.path
import threading
import time
from types import NoneType

import ccxt
import pandas as pd

from datetime import datetime

from BinanceDataManager.decorators import only_implemented_types
from BinanceDataManager.exceptions import WrongExchange, WrongMarket, WrongLimit, WrongTimeframe, WrongSince

class DataManager:
    """
    - Memory centralisation
    - Recover market data from the web
    - Save market data to the local file system
    """

    # constructor
    @only_implemented_types
    def __init__(self, path: (str, NoneType) = "",
                 market: str = "BTC/USDT",
                 timeframe: str = "1d",
                 since: int = 1640991600,
                 limit: int = 1000,
                 multithreading: bool = True,
                 download_size: int = 100):
        """
        Initialize the DataManager
        :param path: path for file containing market data, None if you don't want the manager to save your data
        :param market: "BTC/USDT" for example, depending on the exchange
        :param timeframe: usually "1m", "5m", "15m", "30m", "1h", "1d", "1w", "1M", "1y"
        :param since: the timestamp of the first candle to download
        :param limit: the number of candles to download
        :param multithreading: False to disable multithreading download
        :param download_size: Amount of requests scheduled at the same time if you are encountering issues disable it or
         lower the amount of requests scheduled at the same time using
        size parameter, if you want to increase the download speed increase this parameter but this is not recommended
        and can cause issues.
        """

        # Exchange
        self.exchange = ccxt.binance()
        self.max_candles_per_request = 1000  # max number of candles returned per request
        # Market data
        self.market = market  # market name
        self.timeframe = timeframe  # usually "1m", "5m", "15m", "30m", "1h", "1d", "1w", "1M", "1y" depending on the
        self.since = since  # timestamp
        self.limit = limit  # number of candles to get
        # Used to download data
        self.multithreading = multithreading  # toggle multithreading download
        self.download_size = download_size  # set the amount of requests sent at the same time
        # Data file
        self.path = path  # path for the data file
        self.ready_to_load = False  # true when data file exist and is ready to be loaded
        if self.path is None:
            self.file_enable = False  # disable file saving and loading
        else:
            self.file_enable = True  # enable it

            # Check if a file containing the data exist or if we have to download the data
            since = datetime.fromtimestamp(self.since).strftime("[%d-%m-%y-%H-%M-%S]")
            filename = f"{market.replace('/', '')}_{since}_{self.limit}_candles_{timeframe}.csv"
            self.path += filename
            if os.path.exists(self.path):
                self.ready_to_load = True
                return

        # Main dataframe containing all the market data
        self.main_dataframe = None  # Pandas dataframe containing all the market data

        # Test if the exchange is valid
        try:
            self.exchange.load_markets()
        except Exception as e:
            raise WrongExchange(self.exchange)

        # Test if the market is valid
        try:
            self.exchange.fetch_ticker(self.market)
        except Exception as e:
            raise WrongMarket(self.market)

        # Test if the timeframe is valid
        try:
            data = self.exchange.fetch_ohlcv(symbol=self.market, timeframe=self.timeframe, limit=1)
        except Exception as e:
            raise WrongTimeframe(self.timeframe)

        # Test if the limit is valid
        try:
            self.exchange.fetch_ohlcv(self.market, self.timeframe, limit=self.limit)
        except Exception as e:
            raise WrongLimit(self.limit)

        # Some exchanges need a timestamp with microseconds, so we check if it's required with this exchange
        data = self.exchange.fetch_ohlcv(symbol=self.market, timeframe=self.timeframe, since=self.since * 1000,
                                         limit=10)
        now = time.time()
        if len(str(data[0][0])) != len(str(int(now))):
            self.since *= 1000

    # Recover market data
    def load(self):
        """
        Load market data from file or download it
        """
        if not self.file_enable:  # just download data
            self.main_dataframe = self.__download_data__()
        elif self.ready_to_load:
            print("Loading data from file")
            self.main_dataframe = self.__load_data__()  # load data from file
        else:
            self.main_dataframe = self.__download_data__()  # download data and save it
            self.main_dataframe.to_csv(self.path)

        print("Your data have being loaded into the DataManager")

    # download market data
    def __download_data__(self):
        """
        Download market data from the exchange api
        :return: a dataframe containing all the market data
        """

        def merge_dataframes(main: pd.DataFrame, df_to_merge: pd.DataFrame):
            """
            - Merge two dataframes, remove the last line of the first df

            :param main: main dataframe
            :param df_to_merge: dataframe to merge
            :return: a new dataframe
            """
            if main.empty:
                return df_to_merge

            # remove the last line of the main dataframe
            main.drop(len(main) - 1, axis=0, inplace=True)
            # merge the two dataframes
            merged = pd.concat([main, df_to_merge], axis=0)
            merged.reset_index(drop=True, inplace=True)
            return merged

        remaining_limit = self.limit
        timestamp = self.since

        # We download here a first set of candles
        dataframe = pd.DataFrame(self.exchange.fetch_ohlcv(self.market, self.timeframe, timestamp, remaining_limit))

        remaining_limit -= 1000
        # We check if we downloaded everything
        if remaining_limit <= 0:
            # We return the dataframe
            return dataframe

        # if not, we will generate all timestamps to schedule all requests in different threads
        timestamp_0 = dataframe.iloc[0][0]  # first timestamp
        timestamp_l1 = dataframe.iloc[-1][0]  # last timestamp
        offset = timestamp_l1 - timestamp_0  # offset between the two timestamps
        data = []  # each values contains first a timestamp an then a limit
        current_timestamp = timestamp_0  # current timestamp
        while remaining_limit > 0:
            # we generate the timestamps
            current_timestamp += offset
            data.append([current_timestamp, remaining_limit + 1])
            remaining_limit -= 999

        temp_responses = {}  # temporary responses
        responses = {}  # list of responses
        expected_length = len(data)  # expected length of the responses list
        total_length = expected_length  # total length of the responses list
        if expected_length > self.download_size:
            expected_length = self.download_size

        def download(ts, lt, rid):
            """
            - Download market data from the exchange api
            :param ts: since parameter
            :param lt: limit parameter
            :param rid: request id
            """
            downloaded = self.exchange.fetch_ohlcv(symbol=self.market, timeframe=self.timeframe, since=int(ts), limit=lt)
            temp_responses[rid] = (pd.DataFrame(downloaded))

        current_rid = 0  # current request id
        # we schedule all requests
        if self.multithreading:
            scheduled = 0
            for timestamp, limit in data:
                threading.Thread(target=download,
                                 args=(timestamp, limit, current_rid)).start()  # we schedule the request
                current_rid += 1  # we increment the request id
                scheduled += 1  # we increment requests amount

                if scheduled == expected_length:  # we wait for the responses
                    while not len(temp_responses) == expected_length:
                        pass
                    responses.update(temp_responses)  # we merge new responses with the old ones
                    temp_responses = {}  # we reset temporary responses
                    scheduled = 0
                    expected_length = len(data) - len(responses)  # expected length of the responses list
                    if expected_length > self.download_size:
                        expected_length = self.download_size
                    print("\n" * 50)
                    print("Multithreading Download")
                    print(f"Scheduling {total_length} requests")
                    p = int(current_rid / total_length * 100)
                    n = int(p / 4)
                    print(f"[{n * chr(9608)}{(25 - n) * '-'}] {p}%")
                    # handling here rate limit
                    current_weight = int(self.exchange.last_response_headers['x-mbx-used-weight-1m'])
                    if current_weight + self.download_size >= 1200:
                        remaining = 60
                        for i in range(12):
                            print("\n" * 50)
                            print("Multithreading Download")
                            print(f"Scheduling {total_length} requests")
                            print(f"[{n * chr(9608)}{(25 - n) * '-'}] {p}%")
                            print(f"Rate limit reached, waiting for {remaining}s")
                            remaining -= 5
                            time.sleep(5)

                        print("Resuming download")
        else:
            for timestamp, limit in data:
                download(timestamp, limit, current_rid)
                current_rid += 1  # we increment the request id
                print("\n" * 50)
                print("Single-thread Download")
                print(f"Scheduling {total_length} requests")
                p = int(current_rid / total_length * 100)
                n = int(p / 4)
                print(f"[{n * chr(9608)}{(25 - n) * '-'}] {p}%")
                current_weight = int(self.exchange.last_response_headers['x-mbx-used-weight-1m'])
                if current_weight + 1 >= 1200:
                    remaining = 60
                    for i in range(12):
                        print("\n" * 50)
                        print("Single-thread Download")
                        print(f"Scheduling {total_length} requests")
                        print(f"[{n * chr(9608)}{(25 - n) * '-'}] {p}%")
                        print(f"Rate limit reached, waiting for {remaining}s")
                        remaining -= 5
                        time.sleep(5)

                    print("Resuming download")

        # we merge all the dataframes
        print("\n" * 50)
        print("Multithreading Download")
        print(f"Scheduling {total_length} requests")
        print(f"[{25 * chr(9608)}] {100}%")
        print(f"Merging {total_length} dataframes")
        print(f"[{25 * '-'}] {0}%")
        c = 0
        for i in range(total_length):
            dataframe = merge_dataframes(dataframe, responses[i])
            c += 1
            if c == 100:
                c = 0
                p = int(i / total_length * 100)
                n = int(p / 4)
                print("\n" * 50)
                print("Multithreading Download")
                print(f"Scheduling {total_length} requests")
                print(f"[{25 * chr(9608)}] {100}%")
                print(f"Merging {total_length} dataframes")
                print(f"[{n * chr(9608)}{(25 - n) * '-'}] {p}%")

        dataframe.rename(inplace=True, columns={0: 'timestamp',  # renaming columns
                                                1: 'open',
                                                2: 'high',
                                                3: 'low',
                                                4: 'close',
                                                5: 'volume'})

        print("\n" * 50)
        print("Multithreading Download")
        print(f"Scheduling {total_length} requests")
        print(f"[{25 * chr(9608)}] {100}%")
        print(f"Merging {total_length} dataframes")
        print(f"[{25 * chr(9608)}] {100}%")
        print("Renamed columns")

        return dataframe

    # load market data
    def __load_data__(self):
        """
        Load market data from file
        :return: pd dataframe
        """
        return pd.read_csv(self.path)
