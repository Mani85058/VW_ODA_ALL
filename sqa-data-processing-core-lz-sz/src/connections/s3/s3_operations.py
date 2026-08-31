import re
import os
import boto3

from boto3.session import Session
from datetime import datetime, timezone, timedelta

from src.connections.s3.s3 import S3Connector
from src.connections.s3.spark_s3_connect  import s3conn
from src.common.spark_application import SparkApplication
from src.common.sparkdelta.delta_operations import DeltaOperations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession , SQLContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import pyspark.sql.functions as F

from src.common.log.log import Logger
from src.common.util.core_utils import CoreUtils

from pyspark.sql.functions import lit , when, col
from pyspark.sql.functions import explode_outer

from src.common.exceptions import  MissingUsernameOrPassword, EtlException
from pyspark.sql.utils import AnalysisException, IllegalArgumentException


class S3Operations:
    """
    This class covers all processes related to the s3 connection, 
    such as connection, getting table and etc..
    """
    
    def __init__(self, spark, spark_app, config, log):
        
        self._spark = spark
        self._spark_app = spark_app
        self._config = config
        self._log = log
        self._today = datetime.date(datetime.now(timezone.utc))


    def landing_zone_connector(self):
        """
        Objectifies the S3Connection for landing zone
        """
        self._log.info("landing_zone_connector method:")

        return S3Connector(key_id=self._config.get("s3").get("aws_access_key_id"),
        secret_key=self._config.get("s3").get("aws_secret_access_key"),
        endpoint_url=self._config.get("s3").get("endpoint_url"),
        bucket=self._config.get("data").get("lz").get("landing_zone_bucket"))

    def storage_zone_connector(self):
        """
        Objectifies the S3Connection for landing zone
        """
        self._log.info("landing_zone_connector method:")

        return S3Connector(key_id=self._config.get("s3").get("aws_access_key_id"),
        secret_key=self._config.get("s3").get("aws_secret_access_key"),
        endpoint_url=self._config.get("s3").get("endpoint_url"),
        bucket=self._config.get("data").get("sz").get("storage_zone_bucket"))

    def get_csv_folders(self,delta_log_time):
        """
        This method returns the list of csv folder and core files.
        """
        self._log.info("Start of get_csv_folders method:")
        prefix = self._config.get("data").get("lz").get("core")        
        core_file, count_of_files = self.read_and_sort_core_files(self.landing_zone_connector(), prefix, delta_log_time)
        self._log.info("End of get_csv_folders method:")
        return self.get_folder_name(core_file, delta_log_time), count_of_files

    def get_file_folders(self):
        """
        This method returns the list of csv folder and core files.
        """
        self._log.info("Start of get_file_folders method:")
        delta_log_time = None
        prefix = self._config.get("data").get("sz").get("core_delta_log")        
        core_file, count_of_files = self.read_and_sort_core_files(self.storage_zone_connector(), prefix, delta_log_time)
        self._log.info("End of get_file_folders method:")
        return self.get_folder_name(core_file, delta_log_time), count_of_files
    
    def read_and_sort_core_files(self, s3_connector, prefix, delta_log_time):
        """
        This methods extarct the latest uploaded core csv file.
        """ 
        
        self._log.info("Start of read_and_sort_core_files method:")        
        files = s3_connector.get_all_files_in_folder(prefix)     
        files = [obj.key for obj in sorted(files, key=lambda x: x.last_modified.replace(tzinfo=None) >  datetime.strptime(delta_log_time, "%Y-%m-%d"))]  
        return files , len(files)

    def get_folder_name(self, core_file, delta_log_time):
        """
        This method returns the list of csv files and folders.
        """

        csv_folders = []
        self._log.info("Start of get_folder_name method:")
        for latest_file in core_file:
            if latest_file.split("/")[1:2] not in csv_folders:                    
                csv_folders.append(latest_file.split("/")[1:2])
        csv_folders_list = [item for sublist in csv_folders for item in sublist]
        self._log.info("End of get_folder_name method:")

        return csv_folders_list

    
    def latest_files_collector(self, s3_conn, prefix, delta_log_time):
        """
        This method sorts the files in that bucket according to their update with the given prefix
        """

        new_folders = s3_conn.get_all_files_in_folder(prefix)         
        new_files = [obj.key for obj in sorted(new_folders, key=lambda x: x.last_modified.replace(tzinfo=None) >  datetime.strptime(delta_log_time, "%Y-%m-%d"))]  

        return new_files
