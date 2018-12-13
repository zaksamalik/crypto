import os
import pandas as pd
import configparser
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DateType, TimestampType


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
                    .set('spark.driver.extraClassPath', './jars/hadoop-aws-2.7.3.jar'))
    sc = SparkContext(conf=spark_config)

    # parse AWS credentials and pass to hadoop configuration
    if aws_profile:
        config = configparser.ConfigParser()
        config.read(os.path.expanduser("~/.aws/credentials"))
        access_id = config.get(aws_profile, "aws_access_key_id")
        access_key = config.get(aws_profile, "aws_secret_access_key")
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
        hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

    # SQL Context
    sql = SQLContext(sc)

    return sc, sql


@pandas_udf(DateType(), PandasUDFType.SCALAR)
def to_date(target_col):
    return pd.to_datetime(target_col, infer_datetime_format=True, errors='coerce')


@pandas_udf(TimestampType(), PandasUDFType.SCALAR)
def to_timestamp(target_col):
    return pd.to_datetime(target_col, infer_datetime_format=True, errors='coerce')
