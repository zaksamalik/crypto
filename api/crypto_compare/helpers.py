# -*- coding: utf-8 -*-
"""
TODO: docstrings
"""
import itertools
from asyncio_throttle import Throttler
import asyncio
import aiohttp
import time
import json
import pandas as pd


class EndpointBases:
    """
    TODO: docstrings
    """

    def __init__(self):
        """
        TODO: docstrings
        """
        # Price
        self.PRICE_SINGLE = "https://min-api.cryptocompare.com/data/price?"
        self.PRICE_MULTIPLE = "https://min-api.cryptocompare.com/data/pricemulti?"
        self.PRICE_MULTIPLE_FULL = "https://min-api.cryptocompare.com/data/pricemultifull?"
        self.PRICE_CUSTOM_AVERAGE = "https://min-api.cryptocompare.com/data/generateAvg?"
        # Historical Data
        self.HISTORICAL_DAILY_OHLCV = "https://min-api.cryptocompare.com/data/histoday?fsym={0}&tsym={1}"
        self.HISTORICAL_HOURLY_OHLCV = "https://min-api.cryptocompare.com/data/histohour?fsym={0}&tsym={1}"
        self.HISTORICAL_MINUTE_OHLCV = "https://min-api.cryptocompare.com/data/histominute?fsym={0}&tsym={1}"
        self.HISTORICAL_DAY_OHLCV = "https://min-api.cryptocompare.com/data/pricehistorical?fsym={0}&tsyms={1}"
        self.HISTORICAL_DAY_AVERAGE_PRICE = "https://min-api.cryptocompare.com/data/dayAvg?fsym={0}&tsym={1}"
        self.HISTORICAL_DAILY_EXCHANGE_VOLUME = "https://min-api.cryptocompare.com/data/exchange/histoday?tsym={0}"
        self.HISTORICAL_HOURLY_EXCHANGE_VOLUME = "https://min-api.cryptocompare.com/data/exchange/histohour?tsym={0}"
        # Toplists
        self.TOP_VOLUME_24H_FULL = "https://min-api.cryptocompare.com/data/top/totalvolfull?"
        self.TOP_MARKET_CAP_FULL = "https://min-api.cryptocompare.com/data/top/mktcapfull?"
        self.TOP_EXCHANGES_VOLUME_PAIRS = "https://min-api.cryptocompare.com/data/top/exchanges?"
        self.TOP_EXCHANGES_FULL_PAIRS = "https://min-api.cryptocompare.com/data/top/exchanges/full?"
        self.TOP_VOLUME_PAIRS = "https://min-api.cryptocompare.com/data/top/volumes?"
        self.TOP_TRADING_PAIRS = "https://min-api.cryptocompare.com/data/top/pairs?"
        # Other
        self.OTHER_RATE_LIMIT = "https://min-api.cryptocompare.com/stats/rate/limit"
        self.OTHER_RATE_LIMIT_HOUR = "https://min-api.cryptocompare.com/stats/rate/hour/limit"
        self.OTHER_ALL_EXCHANGES_PAIRS = "https://min-api.cryptocompare.com/data/all/exchanges"
        self.OTHER_CCCAGG_EXCHANGES = "https://min-api.cryptocompare.com/data/all/cccaggexchanges"
        self.OTHER_ALL_COINS = "https://min-api.cryptocompare.com/data/all/coinlist"


class HistoricalOHLCV:
    """
    TODO: docstrings
    """

    def __init__(self, app_name, request_type, fsyms, tsyms, limit, all_data=False, exchange='CCCAGG'):
        """
        TODO: docstrings
        :param app_name: (string) Name of application (CryptoCompare recommends this be sent)
        :param request_type: (string) Name of the URL endpoint base from instantiated `EndpointBase` class
        :param fsyms: (list of strings) List of cryptocurrency symbols of interest
        :param tsyms: (list of strings) List of currency symbols to convert into
        :param limit: (integer) The number of data points to return (max 2000)
        :param all_data: (boolean) Return all historical data (param only available for `HISTORICAL_DAILY_OHLCV`)
        :param exchange: exchange to obtain data from (default is CryptoCompare's aggregate `CCCAGG`
        """
        self.app_name = app_name
        self.request_type = request_type
        self.fsyms = fsyms
        self.tsyms = tsyms
        self.url = None
        self.pairs = None
        self.limit = limit
        self.all_data = all_data
        self.exchange = exchange
        self.endpoint_bases = EndpointBases()
        self.responses = []
        self.response_pairs = []
        self.response_df = None

    # instantiate endpoint bases
    endpoint_bases = EndpointBases()

    def run_validation(self):
        """
        TODO:
        :return:
        """
        assert self.request_type in ['HISTORICAL_DAILY_OHLCV',
                                     'HISTORICAL_HOURLY_OHLCV',
                                     'HISTORICAL_MINUTE_OHLCV'], "Invalid request_type: `{}`".format(self.request_type)

    def get_url(self):
        """
        TODO:
        :return:
        """
        url = self.endpoint_bases.__getattribute__(self.request_type)
        if self.all_data:
            url = url + "&limit=2000&toTs={3}"
        else:
            url = url + "&limit={}".format(self.limit)
        url = url + "&e={0}&extraParams={1}".format(self.exchange, self.app_name)
        self.url = url

    def get_pairs(self):
        """
        TODO:
        :return:
        """
        self.pairs = itertools.product(self.fsyms, self.tsyms)

    async def get_historical(self):
        """
        TODO:
        :return:
        """
        throttler = Throttler(rate_limit=290, period=60)
        if self.all_data:
            # TODO:
            print('a')
        else:
            async with aiohttp.ClientSession() as session:
                for pair in self.pairs:
                    async with throttler:
                        async with session.get(self.url.format(pair[0], pair[1], self.limit)) as resp:
                            if resp.status == 200:
                                self.responses.append(await resp.text())
                                self.response_pairs.append(pair)
                        await asyncio.sleep(0.01)

    def responses_to_dfs(self):
        """
        TODO:
        :return:
        """
        response_df_list = []
        for response, pair in list(zip(self.responses, self.response_pairs)):
            data = json.loads(response)['Data']
            if data:
                response_df = pd.DataFrame(data)
                response_df['fsym'] = pair[0]
                response_df['tsym'] = pair[1]
                response_df_list.append(response_df)
        assert len(response_df_list) > 0, "~~~ None of the successful responses contain new data! ~~~"

        self.response_df = pd.concat(response_df_list)

    def run(self):
        """
        TODO:
        :return:
        """
        # run validations
        self.run_validation()
        # get url
        self.get_url()
        # get pairs to pull from CryptoCompare API
        self.get_pairs()
        # asynchronously pull CryptoCompare historical data
        print("~~~ Pulling Historical OHLCV data ~~~")
        start_time = time.time()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.get_historical())
        print("~~~ Historical OHLCV data pulled in: %s minutes ~~~" % round((time.time() - start_time) / 60, 2))
        # convert repsonses to concatenated Pandas DataFrame
        self.responses_to_dfs()
