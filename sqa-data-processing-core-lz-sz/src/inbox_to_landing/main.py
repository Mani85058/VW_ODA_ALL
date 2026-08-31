import argparse
import yaml
import sys
import os
import re

import urllib3
urllib3.disable_warnings()

from src.common.log.log import Logger
from src.inbox_to_landing.inbox_mover import InboxMover
from src.common.config import Config
from src.common.util.core_utils import CoreUtils
from pyspark.sql.utils import AnalysisException, IllegalArgumentException
from src.common.exceptions import ConfigFileNotFoundError, SoureNameError, MissingUsernameOrPassword,SourcePathAndColumn, WrongTimeFormat, EtlException, NoFilesFoundException

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
    Main function for copies csv files from inbox to lz.
    """

    
    with Logger(int(args.app_log_level), args.log_bucket_name, args.log_folder_name, args.app_name, args.jobname) as log :
        try:
            log.info("Starting application: " + args.jobname)
            config = Config.get_config(args.config)
            inbx_mvr = InboxMover(log, config)
            report = inbx_mvr.main()
            log.set_flush_limit(1)

            if not report is None:
                log.info("status: " + report["status"])
                if report["status"] == "OK":
                    log.info("status: " + report["status"])
                    if report["total"] == 0:
                        raise NoFilesFoundException("No core csv data files received from RVS in inbox") 
                    log.info("total:" + str(report["total"]))
                    log.info("duplicates:" + str(len(report["duplicates"])))

        except ConfigFileNotFoundError as err:                     
            raise ConfigFileNotFoundError("Config file not found ", CoreUtils.concat_err_msg_stacktrace(err))

        except IllegalArgumentException as err:                       
            raise SoureNameError("Wrong Source path in ETL, path is not specified", CoreUtils.concat_err_msg_stacktrace(err))
        
        except KeyError as err:            
            raise MissingUsernameOrPassword("Missing user name or password or URL", CoreUtils.concat_err_msg_stacktrace(err))

        except WrongTimeFormat as err:                       
            raise WrongTimeFormat("Time format is wrong", CoreUtils.concat_err_msg_stacktrace(err))

        except AnalysisException as err:            
            raise SourcePathAndColumn("Wrong Source path in configuartion/ Wrong selection of columns", CoreUtils.concat_err_msg_stacktrace(err))

        except NoFilesFoundException as err:
            raise NoFilesFoundException("No core csv data files received from RVS in inbox", CoreUtils.concat_err_msg_stacktrace(err) )

        except Exception as err:                       
            raise EtlException("Exception in ETL", CoreUtils.concat_err_msg_stacktrace(err)) 


    if report["status"] == "OK":
        log.info("Report created, migration successful")
