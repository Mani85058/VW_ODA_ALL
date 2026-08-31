import yaml
import logging

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession , SQLContext
from pyspark.sql import functions as f

from src.common.spark_application import SparkApplication
from src.connections.s3 import S3Connector
from src.common.log import Logger
import argparse
import logging.config
import sys
import json
import yaml
import os
import csv
from requests_pkcs12 import get

from src.connections.http_connector import HttpConnector
from src.common.conbase_utility import ConbaseUtility
from src.common.spark_application import SparkApplication
from src.common.log import Logger
from src.connections.spark_s3_connector import Connectors
from src.ingestion.conbase_ingest import ConbaseIngest

class IdCollector:
    """
    This util class can be used to send http requests.
    It requires a certificate for authentication. Optional parameters are certificate verification and a default URL.
    """
    @staticmethod
    def id_collector(config, log):
        """
        This method runs the spark job for collecting product ids from bucket and converts it as a csv output.
        """       
        spark_app = SparkApplication()
        spark = spark_app.create_spark_app()   
        Connectors.s3connect(spark)  
        log.info('starting delta file reading...')
        core_header_list = ['modell','modelljahr','fahrzeugklasse','produktkennzeichen','konzernprojektbezeichnung']
        core = spark_app.read_csv_rvs(spark, config.get("data").get("lz").get("core"), core_header_list)
        core_ids = core.select('produktkennzeichen').distinct()
        list_of_ids = [str(row.produktkennzeichen) for row in core_ids.collect()]
        return list_of_ids
