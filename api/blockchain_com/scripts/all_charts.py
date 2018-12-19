# -*- coding: utf-8 -*
"""Gets all chart data from `blockchain.com` and loads it to S3.
"""
import time
from multiprocessing import Pool

import pandas as pd
import requests as req

from api.blockchain_com.helpers import get_chart_names_url_bases
from helpers.aws import df_to_s3
from helpers.general import get_utc_ts_str


def get_chart_data(chart_url_with_name):
    """Gets `blockchain.com` chart data and uploads to S3.

    Args:
        chart_url_with_name (tuple): (URL and corresponding name for given `blockchain.com` chart.

    Returns: Chart API data uploaded as Parquet file to S3.

    """

    url = chart_url_with_name[0]
    chart_name = chart_url_with_name[1]
    request_ts = get_utc_ts_str()

    resp = req.get(url=url)
    resp_json = resp.json()
    df = pd.DataFrame(resp_json['values'])

    df_to_s3(df=df,
             target_bucket='data.crypto',
             folder_path='api/blockchain.com/_' + chart_name,
             file_name=request_ts,
             print_message=False)

    return 0


def main():
    timespan = 'all'
    data_format = 'json'

    # get S3 file paths (+ chart names) and URLs to request chart data from `blockchain.com`
    chart_names, url_bases = get_chart_names_url_bases()
    url_suffix = "?timespan={0}&format={1}".format(timespan, data_format)
    chart_urls = [u + url_suffix for u in url_bases]
    chart_urls_with_names = zip(chart_urls, chart_names)

    print("~~~ Pulling `blockchain.com` chart data. ~~~")
    start_time = time.time()
    pool = Pool()
    pool.map(get_chart_data, chart_urls_with_names)
    print("~~~ All chart data pulled in: %s minutes ~~~" % round((time.time() - start_time) / 60, 2))


if __name__ == '__main__':
    main()
