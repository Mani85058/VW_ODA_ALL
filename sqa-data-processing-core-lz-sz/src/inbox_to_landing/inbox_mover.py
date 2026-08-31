import argparse
from datetime import datetime
import yaml
import time
import json
import sys
import os
import re
import boto3, botocore
from botocore.exceptions import ClientError

import urllib3
urllib3.disable_warnings()

from src.common.log.log import Logger
from src.connections.s3.spark_s3_connect import s3conn
from src.common.config import Config
from src.common import env
from src.common.util.boto3.search import boto3_search
from src.common.util.core_utils import CoreUtils
from src.common.exceptions import BotoCoreException

class InboxMover:

    def __init__(self, log, config):
        self.log = log
        self.config = config

    def main(self):
        """
        This method performs file transfer from inbox to sz as a result of information obtained from config and env
        """

        s3_source_bucket = self.config.get('data').get("inbox").get("bucket")
        s3_source_path = self.config.get('data').get("inbox").get("path")

        s3_target_bucket = self.config.get('data').get("lz").get("landing_zone_bucket")
        s3_target_path = self.config.get('data').get("lz").get("core")
        s3_target_duplicates_path = self.config.get('data').get("lz").get("duplicates_path")

        s3_file_pattern = self.config.get('data').get("inbox").get("file_pattern")
        s3_target_format = self.config.get('data').get("lz").get("format")
        delete_source_files = self.config.get('data').get("inbox").get("delete_source")

        config_duplicates_strategy = self.config.get('data').get("lz").get("duplicated_strategy")

        self.log.info("config S3_SOURCE_BUCKET: " + str(s3_source_bucket))
        self.log.info("config S3_SOURCE_PATH: " + str(s3_source_path))
        self.log.info("env SOURCE_S3_ENDPOINT_HTTPS_URL: " + str(env.get('S3_ENDPOINT_URL')))

        
        self.log.info("config S3_TARGET_BUCKET: " + str(s3_target_bucket))
        self.log.info("config S3_TARGET_PATH: " + str(s3_target_path))

        self.log.info("config FILE_PATTERN: " + str(s3_file_pattern))

        self.log.info("config DELETE_SOURCE_FILES: " + str(delete_source_files))
        self.log.info("config DUPLICATES_STRATEGY: " + str(config_duplicates_strategy))
        
        core_utils = CoreUtils(self.log)
        try:

            s3_client = core_utils.get_boto3client(
                url = env.get('S3_ENDPOINT_URL'),
                user = env.get('AWS_ACCESS_KEY_ID'),
                password = env.get('AWS_SECRET_ACCESS_KEY')
            )

            src_filelist_gen = boto3_search.get_matching_s3_keys(
                client = s3_client,
                bucket = s3_source_bucket,
                prefix = s3_source_path,
                suffix = ''
            )

            target_filelist_gen = boto3_search.get_matching_s3_keys(
                client = s3_client,
                bucket = s3_target_bucket,
                prefix = s3_target_path,
                suffix = ''
            )

            target_duplicates_list = boto3_search.get_matching_s3_keys(
                client = s3_client,
                bucket = s3_target_bucket,
                prefix = s3_target_duplicates_path,
                suffix = ''
            )

        except botocore.exceptions.ClientError:
            self.log.error("Enable to list s3")
            raise BotoCoreException("Enable to list s3", CoreUtils.concat_err_msg_stacktrace(err_boto))

        except botocore.exceptions.ParamValidationError as err_boto:
            self.log.error("ERROR: The parameters for boto3client are wrong")
            raise BotoCoreException("The parameters for boto3client are wrong", CoreUtils.concat_err_msg_stacktrace(err_boto))
            
        cleaned_list = [x for x in src_filelist_gen if "dummy" not in x]

        s3_files = []
        for key in cleaned_list:

            source_filename = key.replace(s3_source_path, '')[11:]
            target_path = s3_target_path + key.replace(s3_source_path, '')[0:11].replace('-', '/')
            
            if source_filename[-4:] != s3_target_format:
                target_filename = source_filename + s3_target_format
            else:
                target_filename = source_filename

            s3_files.append({
                'source_bucket': s3_source_bucket,
                'source_path': s3_source_path + key.replace(s3_source_path, '')[0:11],
                'source_file': source_filename,
                'target_bucket': s3_target_bucket,
                'target_path': target_path,
                'target_file': target_filename
            })
        
        list_filtered = core_utils.filter_file_list(
            list = s3_files,
            pattern = str(s3_file_pattern)
        )
        
        report = core_utils.move_files(
            s3_client = s3_client,
            source_list = list_filtered,
            target_list = target_filelist_gen,
            target_duplicates_list = target_duplicates_list,
            target_base_path = s3_target_path,
            target_duplicates_path_base = s3_target_duplicates_path,
            duplicates_strategy = config_duplicates_strategy,
            delete_source_files = delete_source_files,
            log = self.log
        )
        
        return report