import time

import pyspark.sql.functions as sqlf
from pyspark.sql.functions import col, expr

import etl_spark.helpers as etl


def main():
    # get Spark contexts
    sc, sql_context = etl.get_spark_context(d_mem='2g', e_mem='4g', aws_profile='default')

    # load raw data
    minute_ohlcv_raw = (
        sql_context
            .read
            .parquet("s3a://data.crypto/api/cryptocompare.com/historical/ohlcv/minute/_2018-12-18_00_00_00/*")
    )

    # cast columns
    double_cols = ['open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
    minute_ohlcv_cast = (
        minute_ohlcv_raw
            .select(*(col(c).cast('double').alias(c) if c in double_cols else c for c in minute_ohlcv_raw.columns))
            .withColumn('date_hour', etl.unix_time_to_ts(col('time').cast('int')))
            .withColumn('hour', sqlf.hour(col('date_hour')))
            .withColumn('minute', sqlf.minute(col('date_hour')))
            .withColumn('date', col('date_hour').cast('date'))
            .withColumn('request_timestamp', etl.to_timestamp(col('request_timestamp')))
            .withColumnRenamed('volumefrom', 'volume_fsym')
            .withColumnRenamed('volumeto', 'volume_tsym')
            .withColumn('fsym_char1',
                        sqlf.expr("""CASE
                                            WHEN SUBSTRING(fsym, 1, 1) IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) THEN '_'
                                            ELSE SUBSTRING(fsym, 1, 1)
                                        END"""))
    )

    # get minute-over-minute `change` and `pct_change`
    minute_ohlcv_change = (
        minute_ohlcv_cast
            .withColumn('close_lag1', expr("LAG(close) OVER(PARTITION BY fsym, tsym ORDER BY date_hour)"))
            .withColumn('change', col('close') - col('close_lag1'))
            .withColumn('pct_change', col('change') / col('close_lag1'))
    )

    minute_ohlcv_final = (
        minute_ohlcv_change.select(
            ['date',
             'hour',
             'minute',
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
    (minute_ohlcv_final
     .sort('date')
     .repartition('fsym_char1', 'tsym')
     .write
     .partitionBy(['fsym_char1', 'tsym'])
     .parquet("s3a://etl.analysis/api/cryptocompare.com/historical/ohlcv/minute/_2018-12-18_00_00_00.parquet"))
    end_time = time.time()
    print("~~~ OHLCV Minute - completed in: %s minutes ~~~" % round((end_time - start_time) / 60, 2))


if __name__ == '__main__':
    main()
