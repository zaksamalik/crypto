# -*- coding: utf-8 -*
"""Contains 'helper' functions used across `crypto` repository.

Attributes:
    -`df_to_s3`: function to upload Pandas DataFrame to AWS S3.
    - `json_to_s3`: function to upload API response(s) as JSON objects to S3.
"""
import json
import re

import boto3
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
    print("`df_to_s3`: Successfully uploaded to S3 bucket: `{}`".format(re.sub('s3://', '', output_file)))


def json_to_s3(response, target_bucket, file_path, file_name, multiple):
    """Upload API response(s) to S3 as JSON object(s).

    If `multiple` set to true --> `response`, `file_path`, and `file_name` should be lists.
        > function will iteratively upload each response to S3.

    Args:
        response (str or list): contains responses
        target_bucket (str): name of target S3 bucket
        file_path (str or list): contains file paths (excluding file name) where data will be written
        file_name (str or list): name of `.json` file.
        multiple (bool): Whether to write multiple responses.

    Returns:

    """
    s3 = boto3.resource('s3')
    # handle multiple responses passed as list
    if multiple:
        # check input types
        assert all([type(response) is list, type(file_path) is list, type(
            file_name) is list]), "`multiple` set to true but `response`, `file_path`, and `file_name` are not lists."
        for resp, resp_fp, resp_ts in zip(response, file_path, file_name):
            data = json.loads(resp)
            obj = s3.Object(target_bucket, resp_fp + resp_ts + '.json')
            obj.put(Body=json.dumps(data))
        print(f"~~~ Successfully dumped {len(response)} files to S3 bucket `{target_bucket}`! ~~~")
    else:
        data = json.loads(response)
        obj = s3.Object(target_bucket, file_path + file_name + '.json')
        obj.put(Body=json.dumps(data))
        print(f"~~~ Successfully dumped 1 file to S3 bucket `{target_bucket}`! ~~~")
