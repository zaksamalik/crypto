# -*- coding: utf-8 -*
"""Gets all chart data from `blockchain.com` and loads it to S3.
TODO: handle different data formats (csv --> DF --> parquet?)
"""
import asyncio
import time

import aiohttp

from api.blockchain_com.helpers import get_chart_names_url_bases
from helpers.aws import json_to_s3
from helpers.general import get_utc_ts_str


def get_chart_data(s3_folder_path_base='api/blockchain.com/charts/', timespan='all', data_format='json'):
    """Get all data for all charts from `blockchain.com` API.

    Args:
        s3_folder_path_base (str): base folder path within S3 bucket for charts data.
        timespan (str): chart timespan
        data_format (str): (`json` or `csv`)

    Returns:

    """
    # get S3 file paths (+ chart names) and URLs to request chart data from `blockchain.com`
    chart_names, url_bases = get_chart_names_url_bases()
    url_suffix = "?timespan={0}&format={1}".format(timespan, data_format)
    s3_file_paths = [s3_folder_path_base + cn + '/' for cn in chart_names]
    chart_urls = [u + url_suffix for u in url_bases]

    # asynchronously request chart data from `blockchain.com`
    print("~~~ Requesting blockchain.com chart data ~~~")
    start_time = time.now()
    req_ts = get_utc_ts_str()
    responses = []
    resp_file_paths = []
    resp_timestamps = []  # used as S3 file names

    async def get_charts_data(file_paths, urls):
        async with aiohttp.ClientSession() as session:
            for file_path, url in zip(file_paths, urls):
                async with session.get(url) as resp:
                    if resp.status == 200:
                        responses.append(await resp.text())
                        resp_file_paths.append(file_path)
                        resp_timestamps.append(req_ts)

    # execute
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_charts_data(s3_file_paths, chart_urls))

    assert len(responses) == len(chart_names), "~~~ Data for {0} of {1} charts returned. ~~~".format(len(responses),
                                                                                                     len(chart_names))
    print("~~~ Successfully pulled all chart data in : %s minutes ~~~" % round((time.time() - start_time) / 60, 2))

    return responses, resp_file_paths, resp_timestamps


def main():
    # get data
    chart_responses, chart_file_paths, chart_resp_timestamps = get_chart_data()
    # upload to S3
    json_to_s3(response=chart_responses,
               target_bucket='data.crypto',
               file_path=chart_file_paths,
               file_name=chart_resp_timestamps,
               multiple=True)


if __name__ == '__main__':
    main()
