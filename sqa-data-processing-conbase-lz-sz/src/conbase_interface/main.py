import argparse
import logging.config
import sys
import json
import yaml
import os
import csv

from src.connections.http_connector import HttpConnector
from src.common.conbase_utility import ConbaseUtility
from src.common.log import Logger
from src.connections.s3 import S3Connector
from src.connections.spark_s3_connector  import Connectors
from src.ingestion.conbase_ingest import ConbaseIngest
from src.conbase_interface.conbase_lz_sz import LzSzConbase
from src.common.spark_application import SparkApplication

parser=argparse.ArgumentParser()
parser.add_argument('--config', help='Name of the config file',required = True, default = "")
parser.add_argument('--jobname', help='Name of the Job',required = True, default = "")
parser.add_argument('--app_log_level', help='log level for application',required = True, default = 1)
parser.add_argument('--log_bucket_name', help='name of the log bucket',required = True, default = "")
parser.add_argument('--log_folder_name', help='name of the log folder',required = True, default = "")
parser.add_argument('--app_name', help='name of the application',required = True, default = "")
parser.add_argument('--shuffle_partition', help='num of suffle partition',required = False, default = 3)
args=parser.parse_args() 

if __name__ == '__main__':
    """
    Main function for append delta files from lz to sz.
    """
    #Setting up the logger for application level logging
    with Logger(int(args.app_log_level), args.log_bucket_name, args.log_folder_name, args.app_name, args.jobname) as log :
        config = ConbaseUtility.get_config(args.config)        
        log.info('Starting main method for LZ SZ  conbase load')        
        lz_sz_conbase = LzSzConbase(log, config)
        spark_app = SparkApplication()
        spark = spark_app.create_spark_app()
        spark.conf.set("spark.sql.shuffle.partitions",int(args.shuffle_partition))
        Connectors.s3connect(spark)
        log_date = config.get("delta_log").get("date", None)
        lz_sz_conbase.write_conbase_delta(spark, log_date)
        log.info('End of main method for LZ SZ  conbase load')    
