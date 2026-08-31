"""main for ingest of Conbase data"""
import argparse
import logging.config
import sys
import json
import yaml
import os
import csv
import requests

from src.connections.http_connector import HttpConnector
from src.connections.spark_s3_connector  import Connectors
from src.common.conbase_helper import ConbaseHelper
from src.common.conbase_utility import ConbaseUtility
from src.common.spark_application import SparkApplication
from src.common.log import Logger
from src.connections.s3 import S3Connector
from src.ingestion.conbase_ingest import ConbaseIngest
from src.ingestion.productid_collector import IdCollector 

parser=argparse.ArgumentParser()
parser.add_argument('--config', help='Name of the config file',required = True, default = "")
parser.add_argument('--jobname', help='Name of the Job',required = True, default = "")
parser.add_argument('--app_log_level', help='log level for application',required = True, default = 1)
parser.add_argument('--log_bucket_name', help='name of the log bucket',required = True, default = "")
parser.add_argument('--log_folder_name', help='name of the log folder',required = True, default = "")
parser.add_argument('--app_name', help='name of the application',required = True, default = "")
args=parser.parse_args() 

if __name__ == '__main__':
    """
    Main function for Conbase ingestion into S3
    :return:
    """
    #Setting up the logger for application level logging
    with Logger(int(args.app_log_level), args.log_bucket_name, args.log_folder_name, args.app_name, args.jobname) as log :
        
        config = ConbaseUtility.get_config(args.config)
        verify = config.get("http").get("verify", None)
        baseline_overview_url = config.get("http").get("conbase_baseline_overview_url")
        baseline_url = config.get("http").get("conbase_baseline_url")
        log.info('Product id collection is starting ...')
        product_ids = IdCollector.id_collector(config, log)
        log.info('finished taking product ids!')
        log.info('instantiating ConbaseIngest...')
        conbase_products= ConbaseHelper(product_ids, config, baseline_overview_url,  baseline_url, log, verify)
        log.info('starting vehicle_information_collector...')
        conbase_products.vehicle_information_collector()
        log.info('finished ingestion!')
        