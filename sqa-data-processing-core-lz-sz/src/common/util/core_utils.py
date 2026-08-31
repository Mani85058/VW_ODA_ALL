import argparse
from datetime import datetime, timezone
import traceback
import yaml
import time
import json
import sys
import os
import re
import boto3, botocore
from botocore.exceptions import ClientError
from pyspark.sql.functions import substring, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, ArrayType
from pyspark.sql.functions import udf

import urllib3
urllib3.disable_warnings()

from src.common.log.log import Logger
from src.connections.s3.spark_s3_connect import s3conn
from src.common.config import Config
from src.common import env
from src.common.util.boto3.search import boto3_search


class CoreUtils():
    
    def __init__(self, log):
        self._log = log

    def get_boto3client(self, url, user, password):
        """
        It gets boto3 client info
        """
        self._log.info("Get boto client " + url)
        s3 = boto3.client('s3',
                aws_access_key_id = user,
                aws_secret_access_key = password,
                endpoint_url = url,
                use_ssl = True,
                verify = False
        )
        return s3
    
    @staticmethod
    def concat_err_msg_stacktrace(err):
        """
        This methods concatenates error message with stack trace.
        """
        tb = traceback.TracebackException.from_exception(err)
        return(str(err)+ str(tb.stack.format()))

    def get_boto3resource(self, url, user, password):
        """
        It gets boto3 resouce info
        """
        self._log.info("Get boto resource" + url)
        s3 = boto3.resource('s3',
                aws_access_key_id =  user,
                aws_secret_access_key = password,
                endpoint_url = url,
                use_ssl = True,
                verify = False
        )
        return s3

    def show_list(self, list):
        self._log.info("Show list ")
        file_count = 0
        for line in list:
            file_count = file_count + 1
            print(str(line))
        return {
            "status": "OK",
            "total": file_count,
            "duplicates": []
        }

    def s3_is_file_exist(self, s3_client, bucket, key):
        """
        It checks specific file exists on s3
        """
        self._log.info("Get exist files ")
        print('checking ' + key)
        try:
            s3_client.head_object(Bucket = bucket, Key = key)
        except ClientError as e:
            return int(e.response['Error']['Code']) != 404
        return True

    def file_in_list(self, filename, list):
        """
        It checks specific file exists on s3 list
        """

        for s3_file in list:
            if filename == s3_file:
                return True
        return False

    def get_duplicate_version(self, target_duplicates_list, filename):
        """
        This method performs version assigns if the file in the inbox is the same as the file in lz and 
        duplication is allowed.
        """
        self._log.info("Get duplicate version" + filename)
        if self.file_in_list(filename=filename, list=target_duplicates_list):
            version = 0
            while self.file_in_list(filename = filename + "_" + str(version), list = target_duplicates_list):
                version = version + 1
                if version > VERSION_LIMIT:
                    break
            return version
        else:
            return -1
    
    def move_file(
        self,
        s3_client,
        old_bucket_name,
        old_object_name,
        new_bucket_name,
        new_object_name,
        delete_source_files,
        log
        ):
        """
        It determines how the files are copied according to the information provided, 
        and if the delete soirce file is True, it will clean the inbox folder after copying process. 
        """
        
        s3_resource = self.get_boto3resource(
            url = env.get('S3_ENDPOINT_URL'),
            user = env.get('AWS_ACCESS_KEY_ID'),
            password = env.get('AWS_SECRET_ACCESS_KEY')
        )

        copy_source = {
            'Bucket': old_bucket_name,
            'Key': old_object_name
        }

        bucket = s3_resource.Bucket(new_bucket_name)
        bucket.copy(copy_source,  new_object_name)

        if delete_source_files:
            self._log.info("delete " + old_bucket_name + "/" + old_object_name)
            s3_resource.Object(old_bucket_name, old_object_name).delete()

        return True


    def move_files(
        self,
        s3_client, 
        source_list,
        target_list,
        target_duplicates_list,
        target_base_path,
        target_duplicates_path_base,
        duplicates_strategy,
        delete_source_files,
        log 
        ):
        """       

        s3_client; Boto3 Client: s3 client for file operations
        source_list; Array of Dictionaries: list of files from the source
        target_list; Array of Strings: list of existing file at the target for faster duplication detection
        target_duplicates_list; Array of Strings: list of existinf file in duplicates folder for versioning if required
        target_base_path; String: where to save the files
        target_duplicates_path_base; String: where to store the duplicates
        duplicates_strategy; String: what to do with the duplicates: skip, version, overwrite
        delete_source_files; Boolean: delete the source files after the momevent or not,
        log; log
        """
        today = datetime.date(datetime.now(timezone.utc))
        file_count = 0
        duplicates = []
        for s3_file in source_list:
            file_count = file_count + 1

            if self.file_in_list(filename = s3_file["target_path"] + s3_file["target_file"], list = target_list):
                self._log.info("duplicate is found: " + s3_file["target_path"] + s3_file["target_file"])
             
                isvalid_rule = False

                if not isvalid_rule:
                    self._log.error("rule " + duplicates_strategy + " is invalid")
                    continue

                elif duplicates_strategy == "skip":
                    isvalid_rule = True
                    continue

                elif duplicates_strategy == "overwrite":
                    isvalid_rule = True
                    duplicates.append(s3_file["source_file"])
                    target_file = s3_file["target_file"]
                    target_folder =   target_duplicates_path_base + s3_file["target_path"].replace(target_base_path, "")

                elif duplicates_strategy == "version":
                    isvalid_rule = True
                    duplicates.append(s3_file["source_file"])
                    target_folder = target_duplicates_path_base + s3_file["target_path"].replace(target_base_path, "")
                    version = self.get_duplicate_version(target_duplicates_list=target_duplicates_list, filename=target_folder + s3_file["target_file"])
                    version_str = "" if version == -1 else "_" + str(version)
                    target_file = s3_file["target_file"]  + version_str

            else:
                target_file =  s3_file["target_file"]
                target_folder = s3_file["target_path"]
            self._log.info(">> " + target_folder + "/" + target_file)
            self.move_file(s3_client = s3_client,
                old_bucket_name = s3_file["source_bucket"],
                old_object_name = s3_file["source_path"] + s3_file["source_file"],
                new_bucket_name = s3_file["target_bucket"],
                new_object_name = "core/" +str(today) +"/" + target_file,
                delete_source_files = delete_source_files,
                log = log
            )

        return {
            "status": "OK",
            "total": file_count,
            "duplicates": duplicates
        }

    def filter_file_list(self, list, pattern):
        """
        This method filters files based on given pattern
        """
        list_filtered = []
        for element in list:
            filename = element['target_file']
            ismatched = re.search(pattern, filename)
            if ismatched:
                list_filtered.append(element)
        return list_filtered

    def extract_series_and_years_of_model(self, core_data):
        """ 
        This method extracts and adds a series and years to each vehicle.
        """       
        
        core = core_data.withColumn("year_info", F.split(F.regexp_replace(col("Vertriebliche_Klasse"), " ", ""), ",").cast("array<string>"))\
        .withColumn("seperated_info", F.explode(col("year_info")))
        data_with_years = core.withColumn("series",substring(col("seperated_info"),0,2))\
        .withColumn("start_year",substring(col("seperated_info"),4,4).cast(IntegerType()))\
        .withColumn("end_year",substring(col("seperated_info"),9,4).cast(IntegerType()))\
        .select("Produkt","Beschreibung","Produkttyp","series","start_year", "end_year", "Load_Date", "KSU_LOESCH_DATUM")
       
        creator = udf(lambda start_year, end_year: CoreUtils.year_generator(start_year, end_year),ArrayType(IntegerType()))
        data_assigned_list =  data_with_years.withColumn("range_of_years", creator(col("start_year"), col("end_year")))
        data_assigned_years = data_assigned_list.withColumn("modelljahr", F.explode(col("range_of_years"))).drop("start_year", "end_year","range_of_years")
        return data_assigned_years
    
    @staticmethod
    def year_generator(start, end):
        """
        This method creates years between start and end.
        """
        if start is None:
            return []
        elif end is None:
            return [start]
        else:
            return list(range(start, end))   