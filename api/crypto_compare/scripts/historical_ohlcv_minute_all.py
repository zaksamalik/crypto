from api.crypto_compare.helpers import CCHistoricalOHLCV
from api.crypto_compare.scripts.coin_list import get_coin_list


def main():
    # get list --> fsyms
    coin_list = get_coin_list()
    # get all historical minute OHLCV data
    historical_minute = CCHistoricalOHLCV(s3_bucket='data.crypto',
                                          s3_folder_path='api/crypto_compare/historical/ohlcv/minute',
                                          app_name='crypto_learning',
                                          request_type='HISTORICAL_OHLCV_MINUTE',
                                          fsyms=list(coin_list['Symbol']),
                                          tsyms=['USD', 'BTC'],
                                          limit=2000,
                                          all_data=True,
                                          exchange='CCCAGG')
    historical_minute.run()


if __name__ == '__main__':
    main()
