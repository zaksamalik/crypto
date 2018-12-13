"""
TODO: docstrings
"""
import json
import re
from datetime import datetime

import pandas as pd
import requests as req

from api.crypto_compare.helpers import CCEndpointBases
from helpers.aws import df_to_s3


def get_coin_list():
    """ Gets list of coins with coin-level info from CryptoCompare api.

    Returns: Pandas DataFrame containing results from `CCEndpointBases().OTHER_ALL_COINS` response.

    """
    # get all coin information
    coin_list_url = CCEndpointBases().OTHER_ALL_COINS
    resp = req.get(coin_list_url)
    assert resp.status_code == 200, "Bad HTTP responses {}!".format(str(resp.status_code))
    content = json.loads(resp.content.decode('utf-8'))
    # extract data for each coin
    data = content['Data']
    coin_data = [data[c] for c in data.keys()]
    # concatenate to single df
    coin_list_df = pd.DataFrame(coin_data).astype('str')
    return coin_list_df


def main():
    # get timestamp for filename
    file_ts = re.sub('[:. ]', '_', datetime.utcnow().__str__())
    # get coin list df
    coin_list = get_coin_list()
    coin_list['request_timestamp'] = file_ts
    # upload to S3
    df_to_s3(df=coin_list,
             target_bucket='data.crypto',
             folder_path='api/crypto_compare/coin_list',
             file_name=file_ts)


if __name__ == '__main__':
    main()
