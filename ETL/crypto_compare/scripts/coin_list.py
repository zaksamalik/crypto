import ETL.helpers as etl
sc, sqlContext = etl.get_spark_context(d_mem='1g', e_mem='2g', aws_profile='default')


sqlContext.read.parquet("s3n://data.crypto/api/crypto_compare/coin_list/*")
