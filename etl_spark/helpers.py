import configparser
import os
from datetime import datetime

import holidays
import pandas as pd
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DateType, TimestampType, StringType, BooleanType


def get_spark_context(d_mem, e_mem, aws_profile=None):
    """Initiate SparkContext & SQLContext.

    If aws_profile name is provided, will pass AWS credentials to Hadoop configuration.

    Args:
        d_mem (str): driver memory
        e_mem (str): executor memory
        aws_profile (str): name of aws_profile

    Returns: SparkContext (sc) and SQLContext (sql)

    """
    # Spark Context
    spark_config = (SparkConf()
                    .set('spark.driver.memory', d_mem)
                    .set('spark.executor.memory', e_mem)
                    .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
                    .set('spark.driver.extraClassPath', os.environ['JARS_PATH']))
    sc = SparkContext(conf=spark_config)

    # parse AWS credentials and pass to hadoop configuration
    if aws_profile:
        config = configparser.ConfigParser()
        config.read(os.path.expanduser("~/.aws/credentials"))
        access_id = config.get(aws_profile, "aws_access_key_id")
        access_key = config.get(aws_profile, "aws_secret_access_key")

        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.access.key", access_id)
        hadoop_conf.set("fs.s3a.secret.key", access_key)

    # SQL Context
    sql = SQLContext(sc)

    return sc, sql


@pandas_udf(DateType(), PandasUDFType.SCALAR)
def to_date(target_col):
    return pd.to_datetime(target_col, infer_datetime_format=True, errors='coerce')


@pandas_udf(TimestampType(), PandasUDFType.SCALAR)
def to_timestamp(target_col):
    return pd.to_datetime(target_col, infer_datetime_format=True, errors='coerce')


@pandas_udf(DateType(), PandasUDFType.SCALAR)
def date_to_quarter(target_col):
    return target_col + pd.offsets.DateOffset(days=1) - pd.offsets.QuarterBegin(startingMonth=1)


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def day_name(target_col):
    return target_col.apply(lambda x: datetime.strftime(x, "%A"))


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def month_name(target_col):
    return target_col.apply(lambda x: datetime.strftime(x, "%B"))


@pandas_udf(BooleanType(), PandasUDFType.SCALAR)
def is_holiday_usa(target_col):
    return target_col.apply(lambda x: x in holidays.US())


@pandas_udf(TimestampType(), PandasUDFType.SCALAR)
def unix_time_to_ts(target_col):
    return target_col.apply(lambda x: pd.to_datetime(x, unit='s').tz_localize('UTC'))
