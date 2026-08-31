import unittest
import logging
import os
import argparse
import sys
import shutil
import json

from delta.tables import DeltaTable
from datetime import datetime, timezone, timedelta, date
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import explode_outer, col, current_date
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DateType

import pyspark.sql.functions as F

from unittest.mock import Mock, call, patch, mock_open
from unittest import mock
from unittest.mock import patch
from botocore.stub import Stubber

from src.common.util.core_utils import CoreUtils
from src.common.config import Config
from src.common.log.log import Logger
from src.common.spark_application import SparkApplication
from src.connections.s3.s3 import S3Connector
from src.landing_to_storage.core_lz_sz import LzSzCore


class TestLzSzConbase(unittest.TestCase):
    
    LOGGER = logging.getLogger(__name__)

    def test_compare_time(self):
        spark_app = SparkApplication()
        spark = spark_app.create_spark_app()
        config = Mock()
        lzsz = LzSzCore(self.LOGGER, config, spark)      
        
        delta_log_time = "1600-01-01"
        json_time = "2022-01-01"
        compare_result = lzsz.compare_time(delta_log_time, json_time)
        
        json_time  = "1600-01-01"
        delta_log_time= "2022-01-01"
        compare_result_false = lzsz.compare_time(delta_log_time, json_time)                         
        
        self.assertEqual(compare_result, True, " Json time stamp is greater")
        self.assertEqual(compare_result_false, False, " delta_log_time stamp is greater")


    def test_normalization_operations(self):
        spark_app = SparkApplication()
        spark = spark_app.create_spark_app()
        data = spark.read.option("header","true").option("delimiter", ";").csv("tests/files/core_test_normalization.csv")
        config = Mock()
        lzsz = LzSzCore(self.LOGGER, config, spark)
        
        norm_table = lzsz.normalization_operation(spark, spark_app, data)
        norm_cnt = norm_table.count()
        
        self.assertEqual(norm_cnt, 12, "Record count should be 12")


    def test_table_controller(self):
        spark_app = SparkApplication()
        spark = spark_app.create_spark_app()
        config = Mock()
        lzsz = LzSzCore(self.LOGGER, config, spark)
        
        sqamodprod_data = spark.read.option("header","true").option("delimiter", ";").csv("tests/files/files_for_writing/table_controller_modprod.csv")\
        .withColumn("Load_Date", current_date())\
        .withColumn("KSU_LOESCH_DATUM", F.lit(date.today() + relativedelta(years=15)))
        sqamodprod = lzsz.table_controller(spark, sqamodprod_data)
        
        sqamodkpb_data = spark.read.option("header","true").option("delimiter", ";").csv("tests/files/files_for_writing/table_controller_modkpb.csv")\
        .withColumn("Load_Date", current_date())\
        .withColumn("KSU_LOESCH_DATUM", F.lit(date.today() + relativedelta(years=15)))
        sqamodkpb = lzsz.table_controller(spark, sqamodkpb_data)
        
        wrong_table = spark.read.option("header","true").option("delimiter", ";").csv("tests/files/files_for_writing/table_controller_wrong_table.csv")
        wrong = lzsz.table_controller(spark, wrong_table)

        self.assertEqual(sqamodprod, "modprod", "Table should be 'modprod'")
        self.assertEqual(sqamodkpb, "modkpb", "Table should be 'modkpb'")
        self.assertEqual(wrong, None, "Table should be retun None")
    

    def test_controlled_writing_methods(self):
        spark_app = SparkApplication()
        spark = spark_app.create_spark_app()
        config = Config.get_config("staging_test.yaml")
        lzsz = LzSzCore(self.LOGGER, config, spark)
        
        sqamodprod_data = spark.read.option("header","true").option("delimiter", ",").csv("tests/files/files_for_writing/controlled_writing_for_modkpb.csv")
        sqamodprod = lzsz.controlled_writing_for_sqamodprod(spark, sqamodprod_data, False)
        
        sqamodkpb_data = spark.read.option("header","true").option("delimiter", ",").csv("tests/files/files_for_writing/controlled_writing_for_modprod.csv")
        sqamodkpb = lzsz.controlled_writing_for_sqamodkpb(spark, sqamodkpb_data, False)
        
        self.assertEqual(sqamodprod, None, "Table written successfully")
        self.assertEqual(sqamodkpb, None, "Table written successfully")       
    

    def test_convert_time_string(self):
        spark_app = SparkApplication()
        spark = spark_app.create_spark_app()
        config = Config.get_config("staging_test.yaml")
        lzsz = LzSzCore(self.LOGGER, config, spark)
        
        time_string = "2022-03-25"
        time_formatted = lzsz.convert_time_string(time_string)
        
        self.assertEqual(str(type(time_formatted)), "<class 'datetime.datetime'>", "Table written successfully")


if __name__ == "__main__": 
    unittest.main()