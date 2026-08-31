import re
import os
import boto3

from boto3.session import Session
from datetime import datetime, timezone

from src.connections.s3.s3 import S3Connector
from src.connections.s3.spark_s3_connect  import s3conn
from src.common.spark_application import SparkApplication

from delta.tables import DeltaTable
from pyspark.sql import SparkSession , SQLContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import pyspark.sql.functions as F

from src.common.log.log import Logger
from src.common.util.core_utils import CoreUtils
from src.common.exceptions import WrongTimeFormat

from pyspark.sql.functions import lit , when, col
from pyspark.sql.functions import explode_outer

from src.common.exceptions import  MissingUsernameOrPassword, EtlException
from pyspark.sql.utils import AnalysisException, IllegalArgumentException


class DeltaOperations:
    """
    This class covers all of the operations to be done on Delta Tables, 
    such as getting date, checking and etc..
    """

    def __init__(self, spark, spark_app, config, log):
        
        self._spark = spark
        self._spark_app = spark_app
        self._config = config
        self._log = log

    def get_delta_log(self,delta_log_time):
        """
        It extracts delta log time from delta file in S3
        """
        is_initial_load = self.initial_load_writing()
        delta_log_time = self.extract_delta_log_time(delta_log_time, self.initial_load_writing())
        return delta_log_time, is_initial_load

    def initial_load_writing(self):
        """
        This method controls whether to initial load according to the status of both files in the bucket
        """
        if ((DeltaTable.isDeltaTable(self._spark, self._config.get("data").get("sz").get("delta_log_sqamodkpb"))) & \
            (DeltaTable.isDeltaTable(self._spark, self._config.get("data").get("sz").get("delta_log_sqamodprod")))):
            return False
        else: 
            return True

    def extract_delta_log_time(self, delta_log_time, initial_load):
        """
        This method extract delta log time stamp from delta files or from given date or according 
        to initial load.
        """
        self._log.info("Start of extract_delta_log_time method:")  
        if initial_load:
            delta_log_time = '1600-01-01'
        elif (delta_log_time is None):
            try:
                delta_log_files = [self._config.get("data").get("sz").get("delta_log_sqamodprod"),self._config.get("data").get("sz").get("delta_log_sqamodkpb")]
                delta_log_time = self.min_delta_log_time(delta_log_files)
            except AnalysisException :                  
                delta_log_time = None
        else :
            delta_log_time = str(delta_log_time) 
            self._log.info("Using specific date for processing.")
                
        self._log.info("End of extract_delta_log_time method:")
        return delta_log_time

    def min_delta_log_time(self, delta_log_files):
        """
        This method allows the latest delta log file to be found        
        """
        times=[]
        for delta_log_file in delta_log_files:
            delta_log_time = self.read_time_stamp_from_delta_log(delta_log_file)
            times.append(delta_log_time)
        return min(times)

    def read_time_stamp_from_delta_log(self, deltalogfile):
        """
        This methods read the latest timestamp from delta log file.
        """ 
        self._log.info("Start of read_time_stamp_from_delta_log method:")
        core_delta_df = DeltaTable.forPath(self._spark, deltalogfile)    
        core_history = core_delta_df.history(1)
        return self.extract_delta_timestamp(core_history)

    def extract_delta_timestamp(self,core_history):
        """
        This method extracts the timestamp in the delta file and returns that time
        """
        self._log.info('Start of extract delta time method')
        core_history = core_history.select('timestamp').collect()
        timestamp = core_history[0].timestamp
        delta_log_time = str(timestamp).split(' ')[:1]
        delta_log_time = ''.join(delta_log_time)
        self._log.info('End of extract delta time method')
        return delta_log_time 

    def get_update_flag(self, initial_load, table):
        to_update = True
        if initial_load and not DeltaTable.isDeltaTable(self._spark, self._config.get("data").get("sz").get(table)):
                to_update = False
        return to_update