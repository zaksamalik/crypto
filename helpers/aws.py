"""
TODO: docstrings
"""
import re
import pyarrow as pa
import pyarrow.parquet as pq
from s3fs import S3FileSystem


def df_to_s3(df, target_bucket, folder_path, file_name):
    """Converts Pandas DataFrame to PyArrow table --> writes parquet file to S3 bucket.

    Args:
        df (Pandas DataFrame): target Pandas DataFrame to upload as parquet to S3
        target_bucket (str): name of S3 bucket
        folder_path (str): sub-folder path in bucket
        file_name (str): name for parquet file

    Returns: Writes PandasData frame as parquet file to S3 bucket.

    """
    # convert df to PyArrow table
    pa_tbl = pa.Table.from_pandas(df=df)
    # upload as parquet file to S3
    s3 = S3FileSystem()
    output_file = f"s3://{target_bucket}/{folder_path}/{file_name}.parquet"
    pq.write_to_dataset(table=pa_tbl,
                        root_path=output_file,
                        filesystem=s3)
    print("Successfully uploaded DataFrame as Parquet file to S3 bucket: `{}`".format(re.sub('s3://', '', output_file)))
