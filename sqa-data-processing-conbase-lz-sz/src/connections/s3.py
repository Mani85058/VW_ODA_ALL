"""Module containing utilities for S3 access"""
import logging
import os
from collections.abc import Iterable
from typing import Optional

import boto3
from botocore.config import Config
from boto3.session import Session


class S3Connector:
    """
    This util class can be used to transfer data from and to an S3 bucket.
    """

    def __init__(self, key_id: str, secret_key: str, endpoint_url: str, bucket: str,
                 connect_timeout: int = 60, max_attempts: int = 4):
        """
        Constructor for the S3Connector class

        :param key_id: aws access key id
        :param secret_key: aws secret access key
        :param endpoint_url: url of the S3 endpoint
        :param bucket: bucket to store the data
        :param connect_timeout: time in seconds till a timeout exception is thrown when attempting to make a connection
        :param max_attempts: maximum number of retry attempts that will be made on a single request
        see https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
        """
        config = Config(
            connect_timeout=connect_timeout,
            retries={'max_attempts': max_attempts})
        self._session = boto3.Session(aws_access_key_id=os.environ[key_id],
                                      aws_secret_access_key=os.environ[secret_key])
        self._s3 = self._session.resource(service_name='s3', endpoint_url=endpoint_url,
                                          config=config)
        self._bucket = self._s3.Bucket(bucket)
        self._logger = logging.getLogger(name=__name__)

    def read(self, key: str, encoding: str = 'utf-8', rbytes: bool = False) -> Optional[str]:
        """
        read some data from an S3 bucket

        :param key: The key to retrieve from S3 (including folders below bucket level and file name)
        :param encoding: Optional encoding to be used to decode the data. Default is 'utf-8' if not provided
        :param rbytes: Optional param to omit decoding the data
        :return: Data from the S3 bucket as string
        """
        self._logger.info('reading data from s3://%s/%s', self._bucket.name, key)
        try:
            obj = self._bucket.Object(key=key).get().get('Body').read()
            if rbytes:
                return obj
            return obj.decode(encoding, 'backslashreplace')
        except self._session.client('s3').exceptions.NoSuchKey:
            self._logger.info('The file s3://%s/%s does not exist', self._bucket.name, key)
            return None

    def write(self, data: str, key: str, encoding: str = 'utf-8') -> bool:
        """
        Write some data to an S3 bucket

        :param data: The data to store in S3.
        :type data: str
        :param key: The key where to save the data
            (including folders below bucket level and file name)
        :type key: str
        :param encoding: Encoding format which is used to encode the raw data
                         before sending it to S3; default is 'UTF-8'
        :type encoding: str

        :return: True if data was written successfully, False otherwise
        :rtype: bool
        """
        encoded_data = data.encode(encoding)
        self._logger.info('writing data to s3://%s/%s (with encoding %s)', self._bucket.name, key, encoding)
        status_code = self._bucket.put_object(Body=encoded_data, Key=key).get()['ResponseMetadata']['HTTPStatusCode']
        if status_code == 200:
            self._logger.info('s3://%s/%s written successfully', self._bucket.name, key)
            return True
        self._logger.warning('failed to write s3://%s/%s', self._bucket.name, key)
        return False

    def get_all_files_in_folder(self, folder: str) -> Iterable:
        """
        Get all files from a folder in an S3 bucket

        :param folder: Folder to get files from
        :return: Iterable over the contents of the bucket
        """
        self._logger.info('getting all files from s3://%s/%s', self._bucket.name, folder)
        files = self._bucket.objects.filter(Prefix=folder)
        return files

    def copy(self, key_from: str, key_to: str, delete_source: bool = False):
        """
        Copy a file from one s3 folder to another;
        can be used as move file - e.g. optionally delete the source

        :param: key_from: s3 source key
        :param: key_to: s3 destination key
        :param: delete_source: optional flag - if True then the source key is deleted
        """
        copy_source = {
            'Bucket': self._bucket.name,
            'Key': key_from
        }
        # managed transfer which will perform multipart copy if needed
        self._s3.meta.client.copy(copy_source, self._bucket.name, key_to)
        if delete_source:
            self._s3.Object(self._bucket.name, key_from).delete()

    def delete(self, key: str) -> bool:
        """
        Delete the specified file from the S3 bucket

        :param: key: s3 key
        :return: True if deletion was successful, False otherwise
        """
        return self.__delete_object(obj=self._bucket.Object(key=key))

    def delete_recursively(self, folder: str) -> bool:
        """
        Delete all files from the specified folder in the S3 bucket

        :param: key: s3 'folder'
        :return: True if deletion was successful for all keys in folder, False if deletion failed for at least one key
        """
        return_value = True
        for obj in self._bucket.objects.filter(Prefix=folder):
            deleted = self.__delete_object(obj=obj)
            return_value = return_value and deleted
        return return_value

    def __delete_object(self, obj) -> bool:
        deletion_status = obj.delete()['ResponseMetadata']['HTTPStatusCode']
        if deletion_status == 204:
            self._logger.info('deleted %s', obj.key)
            return True
        self._logger.warning('failed to delete %s', obj.key)
        return False

    def upload(self, filename: str, key: str):
        """
        Upload a file to s3

        :param: filename: file to upload
        :param: key: s3 destination key
        """
        self._s3.meta.client.upload_file(filename, self._bucket.name, key)

    @staticmethod
    def create_boto3_client():     
        session = Session(aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'])       
        return session.client(service_name="s3", endpoint_url= os.environ['S3_ENDPOINT_URL'])
    
