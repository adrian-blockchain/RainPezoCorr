import logging
import boto3
from botocore.exceptions import ClientError
import os
import csv


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket.

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """

    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def store_data_s3(data, filename, key, bucket_name):
    """Store data in S3.

    :param data: Data to store
    :param filename: Name of the file to save the data locally
    :param key: Key or object name to use in S3
    :param bucket_name: Name of the S3 bucket
    """
    # Save data to a CSV file
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'precip', 'niveau_nappe_eau'])  # Write the header row
        writer.writerows(data)

    # Upload the file to the S3 bucket
    upload_file(filename, bucket_name, key)
