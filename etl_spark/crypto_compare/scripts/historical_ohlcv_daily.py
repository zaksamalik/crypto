import time

import pyspark.sql.functions as sqlf
from pyspark.sql.functions import col, lit, expr

import etl_spark.helpers as etl


def main():
    # get Spark contexts
    sc, sql_context = etl.get_spark_context(d_mem='2g', e_mem='4g', aws_profile='default')

    # load raw data
    daily_ohlcv_raw = (
        sql_context
            .read
            .parquet("s3a://data.crypto/api/cryptocompare.com/historical/ohlcv/daily/_2018-12-21_00_00_00/*")
    )

    # cast columns
    double_cols = ['open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
    daily_ohlcv_cast = (
        daily_ohlcv_raw
            .select(*(col(c).cast('double').alias(c) if c in double_cols else c for c in daily_ohlcv_raw.columns))
            .withColumn('date', etl.unix_time_to_ts(col('time').cast('int')))
            .withColumn('request_timestamp', etl.to_timestamp(col('request_timestamp')))
            .withColumnRenamed('volumefrom', 'volume_fsym')
            .withColumnRenamed('volumeto', 'volume_tsym')
            .withColumn('fsym_char1',
                        sqlf.expr("""CASE
                                        WHEN SUBSTRING(fsym, 1, 1) IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) THEN '_'
                                        ELSE SUBSTRING(fsym, 1, 1)
                                    END"""))
    )

    # get day-over-day `change` and `pct_change`
    daily_ohlcv_change = (
        daily_ohlcv_cast
            .withColumn('close_lag1', expr("LAG(close) OVER(PARTITION BY fsym, tsym ORDER BY DATE)"))
            .withColumn('change', col('close') - col('close_lag1'))
            .withColumn('pct_change', col('change') / col('close_lag1'))
    )

    # get all-time-high (ATH), pct-from-ATH, and days-since ATH
    daily_ohlcv_atf = (
        daily_ohlcv_change
            .withColumn("all_time_high", expr("MAX(NULLIF(high, 0)) OVER(PARTITION BY fsym, tsym ORDER BY DATE)"))
            .withColumn("pct_from_ath", col("close") / sqlf.col("all_time_high") - lit(1))
    )

    daily_ohlcv_final = (
        daily_ohlcv_atf.select(
            ['date',
             'fsym',
             'tsym',
             'open',
             'high',
             'low',
             'close',
             'change',
             'pct_change',
             'volume_fsym',
             'volume_tsym',
             'all_time_high',
             'pct_from_ath',
             'fsym_char1'])
    )

    start_time = time.time()
    (daily_ohlcv_final
     .sort('date')
     .repartition('fsym_char1', 'tsym')
     .write
     .partitionBy(['fsym_char1', 'tsym'])
     .parquet("s3a://etl.analysis/api/cryptocompare.com/historical/ohlcv/daily/_2018-12-21_00_00_00.parquet"))
    end_time = time.time()
    print("~~~ OHLCV Daily - completed in: %s minutes ~~~" % round((end_time - start_time) / 60, 2))


if __name__ == '__main__':
    main()
