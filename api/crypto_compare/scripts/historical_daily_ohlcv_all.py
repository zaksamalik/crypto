from api.crypto_compare.helpers import CCHistoricalOHLCV
from api.crypto_compare.scripts.coin_list import get_coin_list


def main():
    # get list --> fsyms
    coin_list = get_coin_list()
    # get all historical daily data
    historical_daily = CCHistoricalOHLCV(app_name='crypto_learning',
                                         request_type='HISTORICAL_DAILY_OHLCV',
                                         fsyms=list(coin_list['Symbol']),
                                         tsyms=['USD', 'BTC'],
                                         limit=2000,
                                         s3_folder_path='api/crypto_compare/historical_ohlcv/daily/all',
                                         all_data=True,
                                         exchange='CCCAGG')
    historical_daily.run()


if __name__ == '__main__':
    main()
