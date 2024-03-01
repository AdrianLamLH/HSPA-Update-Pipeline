import boto3
import logging
from botocore.exceptions import ClientError
import os
import sys
import threading
import json

# Progress bar for file upload
class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def start_session():
    # Start session with AWS credentials
    # session = boto3.Session(profile_name="default")

    # S3 where the data is stored
    s3_client = boto3.client('s3')
    return s3_client


def folder_exists_and_not_empty(s3_client, bucket:str, path:str) -> bool:
    '''
    Folder should exist. 
    Folder should not be empty.
    '''
    if not path.endswith('/'):
        path = path+'/' 
    resp = s3_client.list_objects(Bucket=bucket, Prefix=path, Delimiter='/',MaxKeys=1)
    return 'Contents' in resp

def file_exists(s3_client, bucket:str, path:str, object_name:str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=path+object_name)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            print("An error occurred")
            raise
    else:
        # The object does exist.
        return True


def upload_file(s3_client, file_name, bucket, path, object_name=None, verbose=True):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: customizing S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        # Show progress bar if verbose == True
        if verbose:
            response = s3_client.upload_file(file_name, bucket, path+object_name, Callback=ProgressPercentage(file_name))
        else:
            response = s3_client.upload_file(file_name, bucket, path+object_name)
    # Log error if file was not uploaded
    except ClientError as e:
        logging.error(e)
        return False
    return True

# Uploads local file to S3
def file_upload(s3_client,file_name, bucket, path, object_name=None,verbose=True):
    overwrite_confirm = "n"
    # Check if folder directory exists and not empty (i.e. path is correct)
    if folder_exists_and_not_empty(s3_client, bucket, path):
        # Check for existing data in S3
        overwrite_confirm = input(f"\nData already exists in {path}. Do you want to overwrite? (y/n): ")
        if overwrite_confirm.lower() == 'y':
            # Overwrite existing data
            upload_file(s3_client, file_name, bucket, path, object_name=object_name, verbose=verbose)
            print("Data successfully uploaded to S3")
        else:
            print(f"Data was not overwritten.")
    else:
        print(f"Path {path} does not exist or is empty.")

# Write GeoJSON dataframe to S3
def geo_upload_file(s3_client,geo_df, bucket, path, object_name=None):
    s3_client.put_object(
     Body=json.dumps(geo_df.to_json()),
     Bucket=bucket,
     Key=path+object_name
    )

# Updated GeoJSON file upload to S3
def geo_upload(s3_client,geo_df, bucket, path, object_name=None,verbose=True):
    overwrite_confirm = "n"
    # Check if folder directory exists and not empty (i.e. path is correct)
    if folder_exists_and_not_empty(s3_client, bucket, path):
        # Check for existing data in S3
        overwrite_confirm = input(f"\nData already exists in {path}. Do you want to overwrite? (y/n): ")
        if overwrite_confirm.lower() == 'y':
            geo_upload_file(s3_client, geo_df, bucket, path, object_name=object_name)
            print("Data successfully uploaded to S3")
        else:
            print(f"Data was not overwritten.")
    else:
        print(f"Path {path} does not exist or is empty.")