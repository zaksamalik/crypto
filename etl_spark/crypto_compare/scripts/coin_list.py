import etl_spark.helpers as etl

# init SparkContext & SQLContext
sc, sqlContext = etl.get_spark_context(d_mem='1g', e_mem='2g', aws_profile='default')

# read
sqlContext.read.parquet("s3n://data.crypto/api/crypto_compare/all/coinlist/*")
