import argparse
import logging.config
import sys
import json
import yaml
import os
import csv

from src.common.config import Config
from src.common.log.log import Logger
from src.connections.s3.s3 import S3Connector
from src.connections.s3.spark_s3_connect  import s3conn
from src.landing_to_storage.core_lz_sz import LzSzCore
from src.common.spark_application import SparkApplication
from src.common.util.core_utils import CoreUtils
from pyspark.sql.utils import AnalysisException, IllegalArgumentException
from src.common.exceptions import ConfigFileNotFoundError, SoureNameError,SourcePathAndColumn, MissingUsernameOrPassword, WrongTimeFormat, EtlException

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
    with Logger(int(args.app_log_level), args.log_bucket_name, args.log_folder_name, args.app_name, args.jobname) as log :
        try:
            config = Config.get_config(args.config)
            log.info('Starting main method for LZ SZ  core load')        
            spark_app = SparkApplication()
            spark = spark_app.create_spark_app()
            spark.conf.set("spark.sql.shuffle.partitions",int(args.shuffle_partition))
            s3conn.s3connect(config, spark)
            lz_sz_core = LzSzCore(log, config,spark)
            log_date = config.get("delta_log").get("date", None)
            lz_sz_core.write_core_delta(spark, spark_app, log_date)
            log.info('End of main method for LZ SZ core load')

        except FileNotFoundError as err:                     
            raise FileNotFoundError("Config file not found ", CoreUtils.concat_err_msg_stacktrace(err))
        
        except KeyError as err:            
            raise MissingUsernameOrPassword("Missing user name or password or URL", CoreUtils.concat_err_msg_stacktrace(err))

        except WrongTimeFormat as err:                       
            raise WrongDeltaTimeFormat("Time format is wrong", CoreUtils.concat_err_msg_stacktrace(err)) 

        except AnalysisException as err:            
            raise SourcePathAndColumn("Wrong Source path in configuartion/ Wrong selection of columns", CoreUtils.concat_err_msg_stacktrace(err))

        except Exception as err:                       
            raise EtlException("Exception in ETL", CoreUtils.concat_err_msg_stacktrace(err))

        