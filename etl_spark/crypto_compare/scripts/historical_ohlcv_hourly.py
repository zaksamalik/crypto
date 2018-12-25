import time

import pyspark.sql.functions as sqlf
from pyspark.sql.functions import col, expr

import etl_spark.helpers as etl


def main():
    # get Spark contexts
    sc, sql_context = etl.get_spark_context(d_mem='6g', e_mem='4g', aws_profile='default')

    # load raw data
    path = "s3a://data.crypto/api/cryptocompare.com/historical/ohlcv/hourly/_2018-12-23_00_00_00/*"
    hourly_ohlcv_raw = (
        sql_context
            .read
            .parquet(path)
            .repartition('fsym_char1')
            .repartition('fsym_char1', 'fsym', 'tsym')
    )

    # cast columns
    double_cols = ['open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
    hourly_ohlcv_cast = (
        hourly_ohlcv_raw
            .select(*(col(c).cast('double').alias(c) if c in double_cols else c for c in hourly_ohlcv_raw.columns))
            .withColumn('date_hour', etl.unix_time_to_ts(col('time').cast('int')))
            .withColumn('hour', sqlf.hour(col('date_hour')))
            .withColumn('date', col('date_hour').cast('date'))
            .withColumn('request_timestamp', etl.to_timestamp(col('request_timestamp')))
            .withColumnRenamed('volumefrom', 'volume_fsym')
            .withColumnRenamed('volumeto', 'volume_tsym')
    )

    # get hour-over-hour `change` and `pct_change`
    hourly_ohlcv_change = (
        hourly_ohlcv_cast
            .withColumn('close_lag1', expr("LAG(close) OVER(PARTITION BY fsym, tsym ORDER BY date_hour)"))
            .withColumn('change', col('close') - col('close_lag1'))
            .withColumn('pct_change', col('change') / col('close_lag1'))
    )

    hourly_ohlcv_final = (
        hourly_ohlcv_change.select(
            ['date',
             'hour',
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
             'fsym_char1'])
    )

    start_time = time.time()
    (hourly_ohlcv_final
     .sort('date')
     .repartition('fsym_char1', 'tsym')
     .write
     .partitionBy(['fsym_char1', 'tsym'])
     .parquet("s3a://etl.analysis/api/cryptocompare.com/historical/ohlcv/hourly/_2018-12-23_00_00_00.parquet"))
    end_time = time.time()
    print("~~~ OHLCV Hourly - completed in: %s minutes ~~~" % round((end_time - start_time) / 60, 2))


if __name__ == '__main__':
    main()
