from api.crypto_compare.helpers import CCHistoricalOHLCV
from api.crypto_compare.scripts.coin_list import get_coin_list


def main():
    # get list --> fsyms
    coin_list = get_coin_list()
    # get all historical daily data
    historical_daily = CCHistoricalOHLCV(s3_bucket='data.crypto',
                                         s3_folder_path='api/crypto_compare/historical/ohlcv/daily',
                                         app_name='crypto_learning',
                                         request_type='HISTORICAL_OHLCV_DAILY',
                                         fsyms=list(coin_list['Symbol']),
                                         tsyms=['USD', 'BTC'],
                                         limit=2000,
                                         all_data=True,
                                         exchange='CCCAGG')
    historical_daily.run()


if __name__ == '__main__':
    main()
