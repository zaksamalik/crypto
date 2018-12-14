# -*- coding: utf-8 -*
"""Contains 'helper' functions used across `crypto` repository.

Attributes:
    -`df_to_s3`: function to upload Pandas DataFrame to AWS S3.
"""

import json
import re

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from s3fs import S3FileSystem


def create_s3_folders(bucket_name, s3_folder_names):
    """ Creates folders within S3 bucket if they don't exist.

    Args:
        bucket_name (str): Name of target S3 bucket.
        s3_folder_names (list): List containing names of target sub-folders to be created within bucket.

    Returns: Creates sub-folders within target S3 bucket.

    """
    s3 = S3FileSystem()
    # confirm bucket exists
    assert s3.exists(bucket_name), "Target bucket does not exist! Please setup bucket in S3 Management Console."
    # create folders in bucket if they don't exist
    folders_created = []
    for folder in s3_folder_names:
        if not s3.exists(bucket_name + folder):
            s3.mkdir(bucket_name + folder)
            folders_created.append('`' + folder + '`')
    if not folders_created:
        print("~~~ All folders already exist in bucket! ~~~")
    else:
        print("~~~ Successfully created {0} folders in bucket {1}: {2} ~~~".format(len(folders_created),
                                                                                   bucket_name,
                                                                                   ", ".join(folders_created)))


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


def json_to_s3(data, target_bucket, folder_path, file_name):
    """Dump JSON object to S3 bucket.

    Args:
        data (dict): dictionary to be dumped as JSON object to S3.
        target_bucket (str): name of S3 bucket
        folder_path (str): sub-folder path in bucket
        file_name (str): name for JSON file

    Returns: Dumps dict as JSON object to S3 bucket.

    """
    # TODO: create folder if not exists.
    s3 = boto3.resource('s3')
    obj = s3.Object(target_bucket, folder_path + file_name + '.json')
    obj.put(Body=json.dumps(data))
