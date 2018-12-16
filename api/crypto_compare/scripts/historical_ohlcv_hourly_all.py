from api.crypto_compare.helpers import CCHistoricalOHLCV
from api.crypto_compare.scripts.coin_list import get_coin_list


def main():
    # get list --> fsyms
    coin_list = get_coin_list()
    # get all historical hourly OHLCV data
    historical_hourly = CCHistoricalOHLCV(s3_bucket='data.crypto',
                                          s3_folder_path='api/crypto_compare/historical/ohlcv/hourly',
                                          app_name='crypto_learning',
                                          request_type='HISTORICAL_OHLCV_HOURLY',
                                          fsyms=list(coin_list['Symbol']),
                                          tsyms=['USD', 'BTC', 'ETH'],
                                          limit=2000,
                                          all_data=True,
                                          exchange='CCCAGG')
    historical_hourly.run()


if __name__ == '__main__':
    main()
