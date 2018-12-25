# -*- coding: utf-8 -*
"""Contains helpers for interacting with CryptoCompare API.

See documentation: https://min-api.cryptocompare.com/documentation
"""

import itertools
import re
import time
from datetime import datetime, timezone
from multiprocessing import Pool

import numpy as np
import pandas as pd
import requests as req

from helpers.aws import df_to_s3


class CCEndpointBases:
    """Stores URL endpoints for interacting with CryptoCompare api.
    """

    def __init__(self):
        # Price
        self.PRICE_SINGLE = "https://min-api.cryptocompare.com/data/price?"
        self.PRICE_MULTIPLE = "https://min-api.cryptocompare.com/data/pricemulti?"
        self.PRICE_MULTIPLE_FULL = "https://min-api.cryptocompare.com/data/pricemultifull?"
        self.PRICE_CUSTOM_AVERAGE = "https://min-api.cryptocompare.com/data/generateAvg?"
        # Historical Data
        self.HISTORICAL_OHLCV_DAILY = "https://min-api.cryptocompare.com/data/histoday?fsym={0}&tsym={1}"
        self.HISTORICAL_OHLCV_HOURLY = "https://min-api.cryptocompare.com/data/histohour?fsym={0}&tsym={1}"
        self.HISTORICAL_OHLCV_MINUTE = "https://min-api.cryptocompare.com/data/histominute?fsym={0}&tsym={1}"
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


class CCHistoricalOHLCV:
    """Class to interact with CryptoCompare `HISTORICAL_X_OHLCV` endpoints.

    Pulls historical OHLCV data on a daily, hourly, and minute basis from CryptoCompare API.

    `run` function executes all functions in the following order:
        - `run_validation`
        - `get_last_utc_close`
        - `get_url`
        - `get_pairs`
        - `get_historical`

    """

    def __init__(self,
                 s3_bucket, s3_folder_path,
                 app_name, request_type, fsyms, tsyms, limit, all_data=False, exchange='CCCAGG'):
        """Instantiates class with passed variables & sets values for non-passed variables to be used in functions.

        Args:
            s3_bucket (str): Name of S3 bucket to write results of API pull as parquet file.
            s3_folder_path (str): S3 path within bucket to write result.
            app_name (str): Name of application (CryptoCompare recommends this be sent)
            request_type (str): Name of the URL endpoint 'base' from instantiated `EndpointBase` class
            fsyms (list): List of crypto symbols of interest
            tsyms (list): List of currency symbols to convert into
            limit (int): The number of data points to return (max 2000)
            all_data (bool): Whether to return all historical data
            exchange (str): Name of crypto exchange to obtain data (default is CryptoCompare's aggregate `CCCAGG`)
        """
        self.s3_bucket = s3_bucket
        self.s3_folder_path = s3_folder_path
        self.app_name = app_name
        self.request_type = request_type
        self.fsyms = fsyms
        self.tsyms = tsyms
        self.limit = limit
        self.all_data = all_data
        self.exchange = exchange
        # ~~~ Not Passed During Instantiation ~~~
        self.all_data_non_daily = self.all_data & (self.request_type in ['HISTORICAL_OHLCV_HOURLY',
                                                                         'HISTORICAL_OHLCV_MINUTE'])
        self.endpoint_bases = CCEndpointBases()
        self.last_utc_close_ts = None
        self.url = None
        self.pairs = []

    def run_validation(self):
        """Checks that input variable values are valid.

        Returns: First assertion error message for a failed validation check

        """
        # check input types
        assert isinstance(self.app_name, str), "Invalid value provided for `app_name` {}".format(str(self.app_name))
        assert isinstance(self.fsyms, list), "Invalid value provided for `fsyms` {}".format(str(self.fsyms))
        assert all([isinstance(x, str) for x in self.fsyms]), "Invalid list contents in `fsyms` {}".format(str(self.fsyms))
        assert isinstance(self.tsyms, list), "Invalid value provided for `tsyms` {}".format(str(self.tsyms))
        assert all([isinstance(x, str) for x in self.tsyms]), "Invalid list contents in `tsyms` {}".format(str(self.fsyms))
        assert isinstance(self.limit, int), "Invalid value provided for `limit` {}".format(str(self.limit))
        assert isinstance(self.all_data, bool), "Invalid value provided for `all_date` {}".format(str(self.all_data))
        assert isinstance(self.exchange, str), "Invalid value provided for `exchange` {}".format(str(self.exchange))
        # validate `request_type`
        assert self.request_type in ['HISTORICAL_OHLCV_DAILY',
                                     'HISTORICAL_OHLCV_HOURLY',
                                     'HISTORICAL_OHLCV_MINUTE'], "Invalid request_type: `{}`".format(self.request_type)

    def get_last_utc_close(self):
        """Gets most recent UTC close as timestamp (int).

        Value is used in functions:
            `get_historical`: start value passed for `toTs` param in URL endpoint when `self.all_data_non_daily`
            `response_to_df`: filter api response results for records on / before last UTC close

        Returns: `self.last_utc_close_ts` set to last UTC close as timestamp (integer).

        """
        utc_current = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        utc_previous = utc_current - pd.DateOffset(days=1)
        self.last_utc_close_ts = utc_previous.timestamp().__int__()
        self.s3_folder_path = (self.s3_folder_path + '/_' +
                               re.sub('[ :]', '_', pd.to_datetime(self.last_utc_close_ts, unit='s').__str__()))

    def get_url(self):
        """Get URL endpoint with all parameter values except `fsym` and `tsym`.

        Returns: `self.url` set to URL endpoint with all parameter values except `fsym` and `tsym`.

        """
        url = self.endpoint_bases.__getattribute__(self.request_type)
        if self.all_data:
            if self.request_type == 'HISTORICAL_OHLCV_DAILY':
                url = url + "&allData=true"
            else:
                url = url + "&limit=2000&toTs={2}"
        else:
            url = url + "&limit={}".format(self.limit)
        url = url + "&e={0}&extraParams={1}".format(self.exchange, self.app_name)
        self.url = url

    def get_pairs(self):
        """Gets all combinations of `fsyms` and `tsyms`.

        Returns: `self.pairs` set to result of itertools product of `self.fsyms` and `self.tsyms`.

        """
        # get product of fsyms, tsyms
        fsym_tsyms = itertools.product(self.fsyms, self.tsyms)
        fsym_tsym_df = pd.DataFrame(list(fsym_tsyms), columns=['fsym', 'tsym'])
        # get fsym first char
        fsym_tsym_df['fsym_char1'] = fsym_tsym_df['fsym'].apply(lambda x: '_' if re.match("[0-9]", x) else x[0])

        fsym_char1s = np.unique(fsym_tsym_df['fsym_char1'])
        for char1 in fsym_char1s:
            df_sub = fsym_tsym_df[fsym_tsym_df['fsym_char1'] == char1]
            pair = zip(df_sub['fsym'], df_sub['tsym'], df_sub['fsym_char1'])
            self.pairs.append(pair)

    def get_historical_ohlcv(self, pair):
        """Get historical OHLCV data from CryptoCompare API and upload as Parquet file to S3.

           Cannot pass `allData` parameter to CryptoCompare API for non-daily historical OHLCV data -->
           When `all_data_non_daily` = True --> iterate from latest UTC timestamp backwards in batches of 2000.

        Args:
            pair (tuple): e.g. ('BTC', 'USD')

        Returns: Uploads API response data as Parquet file to S3.

        """

        fsym = pair[0]
        tsym = pair[1]
        fsym_char1 = pair[2]
        request_ts = datetime.utcnow().__str__()

        # handle `all_data_non_daily` = True
        if self.all_data_non_daily:
            data_list = []
            # start loop at last UTC close
            to_ts = self.last_utc_close_ts
            while True:
                resp = req.get(self.url.format(fsym, tsym, to_ts))
                if resp.status_code == 200:
                    resp_json = resp.json()
                    data = resp_json['Data']
                    # stop when all records in response have `0` in `volumefrom`
                    all_no_volume = all([x['volumefrom'] == 0 for x in data])
                    if all_no_volume:
                        break
                    else:
                        data_list.append(data)
                        to_ts = resp_json['TimeFrom']
                else:
                    break

            # flatten list
            if data_list:
                data_all = [x for y in data_list for x in y]
                df = pd.DataFrame(data_all)
                df['fsym'] = fsym
                df['tsym'] = tsym
                df['fsym_char1'] = fsym_char1
                df['request_timestamp'] = request_ts
                df_filt = df[df['time'] <= self.last_utc_close_ts]
                return df_filt

        # handle all other requests
        else:
            resp = req.get(self.url.format(fsym, tsym))
            if resp.status_code == 200:
                data = resp.json()['Data']
                if data:
                    df = pd.DataFrame(data)
                    df['fsym'] = fsym
                    df['tsym'] = tsym
                    df['fsym_char1'] = fsym_char1
                    df['request_timestamp'] = request_ts
                    df_filt = df[df['time'] <= self.last_utc_close_ts]
                    return df_filt

    def run_get_historical_ohlcv(self):
        """Runs `get_historical_ohlcv` over all `pairs` in parallel.

        Returns:

        """
        print("~~~ Pulling Historical OHLCV data ~~~")
        start_time = time.time()
        for pair_set in self.pairs:
            # get data for each `fsym_char`, `tsym` set
            pool = Pool()
            ohlcv_df_list = pool.map(self.get_historical_ohlcv, pair_set)
            # concatenate dfs
            if [x for x in ohlcv_df_list if x is not None]:
                ohlcv_df = pd.concat([x for x in ohlcv_df_list if x is not None])
                fsym_char1 = np.unique(ohlcv_df['fsym_char1'])
                assert len(fsym_char1) == 1, "Multiple `fsym_char1s` in data: {}".format(str(fsym_char1))
                # write parquet to S3
                df_to_s3(df=ohlcv_df.sort_values('time').astype('str'),
                         target_bucket='data.crypto',
                         folder_path=self.s3_folder_path,
                         file_name="fsym_char1={}".format(fsym_char1[0]),
                         partition_cols=None,
                         print_message=True)
            pool.close()
        print("~~~ Historical OHLCV data pulled in: %s minutes ~~~" % round((time.time() - start_time) / 60, 2))

    def run(self):
        """Run all functions.

        Returns: API data written as parquet files to target AWS S3 bucket.

        """
        self.run_validation()

        self.get_last_utc_close()

        self.get_url()

        self.get_pairs()

        self.run_get_historical_ohlcv()
