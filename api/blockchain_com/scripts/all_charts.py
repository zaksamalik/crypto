# -*- coding: utf-8 -*
"""Gets all chart data from `blockchain.com` and loads it to S3.
# TODO: handle different timestamps? & data formats (csv --> DF --> parquet?)
"""

import asyncio
import json

import aiohttp

from api.blockchain_com.scripts.create_s3_buckets import get_chart_names_url_bases
from helpers.aws import json_to_s3
from helpers.general import get_utc_ts_str


def get_chart_data(s3_folder_path_base='api/blockchain.com/charts/', timespan='all', data_format='json'):
    """

    Args:
        s3_folder_path_base (str): base folder path within S3 bucket for charts data.
        timespan (str): chart timespan
        data_format (str): (`json` or `csv`)

    Returns:

    """
    # get S3 file paths (w/ chart names) and URLs to request chart data from `blockchain.com`
    chart_names, url_bases = get_chart_names_url_bases()
    url_suffix = "?timespan={0}&format={1}".format(timespan, data_format)
    s3_file_paths = [s3_folder_path_base + cn + '/' for cn in chart_names]
    chart_urls = [u + url_suffix for u in url_bases]

    # asynchronously request chart data from `blockchain.com`
    print("~~~ Requesting blockchain.com chart data ~~~")
    req_ts = get_utc_ts_str()
    responses = []
    resp_file_paths = []
    resp_timestamps = []  # used in S3 file names

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

    assert len(responses) == len(chart_names), "~~~ Data for {0} of {1} charts returned ~~~".format(len(responses),
                                                                                                    len(chart_names))
    print("~~~ Successfully pulled all blockchain.com chart data! ~~~")

    return responses, resp_file_paths, resp_timestamps


def chart_data_to_s3(target_bucket, responses, file_paths, file_names):
    """Dump `blockchain.com` chart data to S3.

    Args:
        target_bucket (str): name of S3 bucket to load data
        responses (list): list of API responses (`responses` returned from `get_chart_data`)
        file_paths (list): list of file paths (`resp_file_paths` returned from `get_chart_data`)
        file_names (list): list of file names (`resp_timestamps` returned from `get_chart_data`)

    Returns: Dumps data returned from `get_chart_data` to S3.

    """
    for resp, resp_fp, resp_ts in zip(responses, file_paths, file_names):
        resp_dict = json.loads(resp)
        json_to_s3(data=resp_dict, target_bucket=target_bucket, folder_path=resp_fp, file_name=resp_ts)

    print("~~~ Successfully loaded all response data to S3!~~~")


def main():
    # get data
    chart_responses, chart_file_paths, chart_resp_timestamps = get_chart_data()
    # upload to S3
    chart_data_to_s3(target_bucket='data.crypto',
                     responses=chart_responses,
                     file_paths=chart_file_paths,
                     file_names=chart_resp_timestamps)


if __name__ == '__main__':
    main()
