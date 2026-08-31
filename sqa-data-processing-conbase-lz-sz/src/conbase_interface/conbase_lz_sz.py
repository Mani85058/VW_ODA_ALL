import re
import os
import boto3
import json

from boto3.session import Session
from datetime import datetime, timezone, timedelta, date
from dateutil.relativedelta import relativedelta

from src.connections.spark_s3_connector import Connectors
from src.connections.s3 import S3Connector
from src.common.spark_application import SparkApplication

from delta.tables import DeltaTable
from pyspark.sql import SparkSession , SQLContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, ArrayType, StructType
import pyspark.sql.functions as F

from src.common.log import Logger
from src.common.conbase_utility import ConbaseUtility

from pyspark.sql.functions import lit , when, col
from pyspark.sql.functions import explode_outer

from src.common.exceptions import  MissingUsernameOrPassword, EtlException
from pyspark.sql.utils import AnalysisException, IllegalArgumentException


class LzSzConbase:
    """
    This class converts new raw conbase file from Json to delta format.
    """
    def __init__(self, log, config):
        """
        Constructor for the ConbaseIngest class.
       
        """
        self.log = log
        self.config = config 

    def landing_zone_connector(self):
        
        return S3Connector(key_id=self.config.get("s3").get("aws_access_key_id"),
        secret_key=self.config.get("s3").get("aws_secret_access_key"),
        endpoint_url=self.config.get("s3").get("endpoint_url"),
        bucket=self.config.get("data").get("lz").get("landing_zone_bucket"))

    def read_time_stamp_from_delta_log(self, spark, deltalogfile):
        """
        This methods read the latest timestamp from delta log file.
        """            
        conbase_delta_df = DeltaTable.forPath(spark, deltalogfile)    
        conbase_history = conbase_delta_df.history(1)
        return self.extract_delta_timestamp(conbase_history)

    def extract_delta_timestamp(self, conbase_history):
        self.log.info('Start of extract delta time method')
        conbase_history = conbase_history.select('timestamp').collect()
        self.log.info('Time variables collected')
        timestamp = conbase_history[0].timestamp    
        delta_log_time = str(timestamp).split(' ')[:1]
        self.log.info('delta log generated')
        delta_log_time = ''.join(delta_log_time)
        self.log.info('End of extract delta time method')
        return delta_log_time
    
    def read_and_sort_conbase_files(self, s3_connector, prefix, delta_log_time):
        """
        This methods extarct the latest uploaded conbase json file.
        """ 
        date_format = "%Y-%m-%d"    

        files = s3_connector.get_all_files_in_folder(prefix)  
        sorted_files = [obj.key for obj in sorted(files, key=lambda x: x.last_modified.replace(tzinfo=None) >  datetime.strptime(delta_log_time, date_format))]  
         
        return sorted_files , len(sorted_files)
    def write_to_delta(self, spark, conbase_json, valid_from, initial_load ):
        """
        Execute the write_to_delta of Conbase data and valid_from_date in five steps:
            Step 1 - Checks the null materials 
            Step 2 - Taking only not null material ones with filter_null_materials
            Step 3 - Creates and removes the required columns from data with obtain_conbase_data
            Step 4 - Extarct the latest uploaded conbase json file with read_and_sort_conbase_files
            Step 5 - Writes the delta files with controlled_writing
        """

        self.log.info("Start of write_to_delta method:")  
        conbase_s3_path = self.config.get("data").get("lz").get("landing_zone")+conbase_json   
        self.log.info('conbase_s3_path geberated')     
        conbase_df_exploded_filtered = self.filter_null_materials(spark, conbase_s3_path)
        self.log.info('conbase_df_exploded_filtered filtered null rows') 
        conbase_attributes_df =  self.obtain_conbase_data(conbase_df_exploded_filtered, valid_from)
       	self.log.info('obtained conbase attributes successfully') 
        self.controlled_writing(spark, conbase_attributes_df, initial_load)
        self.log.info("End of write_to_delta method:")
    
    
    def filter_null_materials(self,spark, conbase_s3_path):
        """
        Checking for materials and taking only not null materials .
        """        

        self.log.info("Start of filter_null_materials method:")  
        conbase_df = spark.read.json(conbase_s3_path)
        self.log.info("Conbase dataframe extracted method:")
        conbase_extracted_df = conbase_df.withColumn("materials", conbase_df.content.materials)\
        .withColumn("productId", conbase_df.content.productId)\
        .withColumn("status", conbase_df.content.status)\
        .select("materials", "productId", "status")\
        .where("upper(status) = 'PUBLISHED'")\
        .drop("status")
        self.log.info("Conbase dataframe extracted column selected:")
        self.log.info("Materials and productId columns created")  
        conbase_df_exploded = conbase_extracted_df.withColumn("materials_items", explode_outer(conbase_extracted_df.materials))
        self.log.info("Materials_items creadted by using explode_outer")
        conbase_df_exploded_filtered = conbase_df_exploded.filter(col("materials_items").isNotNull()).drop("materials")  
        self.log.info("null materials removed")
        conbase_df_exploded_distinct = conbase_df_exploded_filtered.distinct()
        self.log.info("Distinct conbase selected")
        self.log.info("End of filter_null_materials method:")

        return conbase_df_exploded_distinct

    def obtain_conbase_data(self, conbase_df_exploded_filtered, valid_from_date ):
        """ 
        This method creates, and removes the required columns from conbase data 
        """  

        self.log.info("Start of obtain_conbase_data method:")
        self.log.info("Distinct validfrom column generated")
        conbase_attributes_df =  conbase_df_exploded_filtered\
            .withColumn("validfrom", lit(valid_from_date)).distinct()
        self.log.info("Adding KSU Coloumn:")
        self.log.info("KSU column takes current date and ksu date:")
        conbase_attributes_df_ksu_column = conbase_attributes_df\
        .withColumn("KSU_LOESCH_DATUM", F.lit(date.today() + relativedelta(years = +int(self.config.get("ksu").get("period_years")),
        months = +int(self.config.get("ksu").get("period_months")), days = +int(self.config.get("ksu").get("period_days")))))
        self.log.info("KSU Coloumn added:")
        
        self.log.info("Extracting conbase data:")
        conbase_transformed_df = self.obtain_material_attributes(conbase_attributes_df_ksu_column)\
            .select("partNumber", "naming", "materialType", "diagnosisAddresses", "hardwareVersion", "softwareVersion",
            "hardwarePartNumber", "abbreviation","productId", "validfrom","KSU_LOESCH_DATUM")
        self.log.info("Extracting conbase data done:")
        self.log.info("End of obtain_conbase_data method:")
        return conbase_transformed_df
    
    def obtain_material_attributes(self, conbase_attributes_df):
        """
        This method extracts attributes from material if attributes are present.
        """
        self.log.info("Start of obtain_material_attributes method:")
        self.log.info("Start of checking the data strcutres for materials:")
        # self.log.info(f"Type of the material is : {conbase_attributes_df.schema["materials_items"].dataType}")
        # self.log.info(f"Type of the material ecuVersions is : {conbase_attributes_df.materials_items.ecuVersions}")
        if (isinstance(conbase_attributes_df.schema["materials_items"].dataType, StructType)):
            conbase_mat_attrib = conbase_attributes_df\
            .withColumn("partNumber",col("materials_items.partNumber"))\
            .withColumn("naming",col("materials_items.naming"))\
            .withColumn("materialType",col("materials_items.materialType"))\
            .withColumn("diagnosisAddresses",explode_outer(conbase_attributes_df.materials_items.diagnosisAddresses))\
            .withColumn("ecuVersions_items",explode_outer(conbase_attributes_df.materials_items.ecuVersions))
            self.log.info("End of obtain_material_attributes method:") 
            return self.obtain_ecu_attributes(conbase_mat_attrib)
        else:
            self.log.info("Start of obtain_material_attributes method:")
            self.log.info("Start of checking the data strcutres for materials:") 
            return conbase_attributes_df\
            .withColumn("partNumber",lit(None).cast(StringType()))\
            .withColumn("naming",lit(None).cast(StringType()))\
            .withColumn("materialType",lit(None).cast(StringType()))\
            .withColumn("diagnosisAddresses",lit(None).cast(StringType()))\
            .withColumn("hardwareVersion",lit(None).cast(StringType()))\
            .withColumn("softwareVersion",lit(None).cast(StringType()))\
            .withColumn("hardwarePartNumber",lit(None).cast(StringType()))\
            .withColumn("abbreviation",lit(None).cast(StringType()))   

    def obtain_ecu_attributes(self, conbase_mat_attrib):
        """
        This method extracts ecuversions from material if attributes are present.
        """
        self.log.info("Start of obtain_ecu_attributes method:")
        self.log.info("checking  ecu data structures method:")
        if (isinstance(conbase_mat_attrib.schema["ecuVersions_items"].dataType, StructType)):
            self.log.info("End of obtain_ecu_attributes method:")

            conbase_mat_attrib = self.extract_ecu_json(conbase_mat_attrib, "hardwareVersion", "ecuVersions_items.hardwareVersion")
            conbase_mat_attrib = self.extract_ecu_json(conbase_mat_attrib, "softwareVersion", "ecuVersions_items.softwareVersion")
            conbase_mat_attrib = self.extract_ecu_json(conbase_mat_attrib, "hardwarePartNumber", "ecuVersions_items.hardwarePartNumber")
            conbase_mat_attrib = self.extract_ecu_json(conbase_mat_attrib, "abbreviation", "ecuVersions_items.frozenStatus.abbreviation")
            return conbase_mat_attrib
        else:
            self.log.info("checking  ecu data structures method:")
            self.log.info("End of obtain_ecu_attributes method:")
            return conbase_mat_attrib\
            .withColumn("hardwareVersion",lit(None).cast(StringType()))\
            .withColumn("softwareVersion",lit(None).cast(StringType()))\
            .withColumn("hardwarePartNumber",lit(None).cast(StringType()))\
            .withColumn("abbreviation",lit(None).cast(StringType()))

    def extract_ecu_json(self, df, column_name, nested_json_path):
        """
        This method extracts nested json path column and return null if the path is not existed.
        """
        try:
            df_add_col = df.withColumn(column_name,col(nested_json_path))
        except:
            df_add_col = df.withColumn(column_name,lit(None).cast(StringType()))
        return df_add_col

    def controlled_writing(self,spark, conbase_attributes_df , initial_load):
        """
        Performs the execute operation by checking the number of files contained in the conbase data.
        """

        self.log.info("Start of controlled_writing method:")
        conbase_atttributes_nulldropped = conbase_attributes_df.na.drop()
        self.log.info("Conbase attributes filtered from null values")
        conbase_attributes = conbase_atttributes_nulldropped.distinct()
        self.log.info("Conbase attributes filtered from duplicated values")

        if initial_load :
            self.log.info("Start of initial load:") 
            conbase_attributes.write.partitionBy("validfrom")\
            .format(self.config.get("data").get("sz").get("format")).mode("append")\
            .save(self.config.get("data").get("sz").get("delta_log"))
            self.log.info("End of initial load:")
        else:
            delta_table = DeltaTable.forPath(spark, self.config.get("data").get("sz").get("delta_log"))
            delta_table.alias("events").merge(
                conbase_attributes.alias("updates"),
                "events.partNumber =  updates.partNumber AND events.naming = updates.naming AND events.materialType =  updates.materialType AND events.diagnosisAddresses = updates.diagnosisAddresses AND events.hardwareVersion = updates.hardwareVersion AND events.softwareVersion = updates.softwareVersion AND events.hardwarePartNumber = updates.hardwarePartNumber AND events.abbreviation = updates.abbreviation AND events.productId = updates.productId" )\
                .whenNotMatchedInsert(values =
                {
                "productId": "updates.productId",
                "validfrom": "updates.validfrom",
                "partNumber": "updates.partNumber",
                "naming": "updates.naming",            
                "diagnosisAddresses": "updates.diagnosisAddresses",
                "hardwareVersion": "updates.hardwareVersion",
                "softwareVersion": "updates.softwareVersion",
                "hardwarePartNumber": "updates.hardwarePartNumber",
                "abbreviation": "updates.abbreviation",
                "materialType": "updates.materialType",
                "KSU_LOESCH_DATUM": "updates.KSU_LOESCH_DATUM"               
                }
            ) \
            .execute()
        self.log.info("End of controlled_writing method:")  

    def compare_time(self, delta_log_time, json_time):
        """
        This methods compares the date between conbase deltalog file and conbase json file
        and return true if conbase json file is newer than deltalog file.
        """
        self.log.info("Start of compare_time method:")        
        self.log.info("Date format is YYYY-mm-DD ")
        date_time_fromat = "%Y-%m-%d"
        self.log.info("Taking delta time")
        delta_time = datetime.strptime(delta_log_time, date_time_fromat)
        self.log.info("Start of Json time ")
        json_time = datetime.strptime(json_time, date_time_fromat)
        self.log.info("End of compare_time method:")
        if json_time > delta_time :
            self.log.info("Json time is greater than delta time")
            return True
        else:
            self.log.info("delta time is greater than json time")
            return False

    def get_json_folders(self, delta_log_time):
        """
        This method returns the list of json folder and conbase files.
        """
        
        prefix = self.config.get("data").get("lz").get("conbase_folder")
        conbase_file, count_of_files = self.read_and_sort_conbase_files(self.landing_zone_connector(), prefix, delta_log_time)
        
        return self.get_folder_name(conbase_file)
        
    def get_folder_name(self, conbase_file):
        """
        This method returns the list of json files and folders.
        """
        json_folders = []
        self.log.info("Start of get_folder_name method:")
        for latest_file in conbase_file:
            if latest_file.split("/")[1:2] not in json_folders:                    
                json_folders.append(latest_file.split("/")[1:2])
                self.log.info("json_folders updated")
        json_folders_list = [item for sublist in json_folders for item in sublist]
        self.log.info("End of get_folder_name method:")
        return json_folders_list

    def extract_delta_log_time(self,spark, delta_log_time, initial_load):
        """
        This method extract delta log time stamp from delta files or from given date or according 
        to initial load.
        """
        self.log.info("Start of extract_delta_log_time method:")        
        if ((delta_log_time is None) and (not initial_load)):
            try:
                delta_log_file = self.config.get("data").get("sz").get("delta_log")
                delta_log_time = self.read_time_stamp_from_delta_log(spark, delta_log_file)
            except AnalysisException :                  
                delta_log_time = None
        elif initial_load :
            self.log.info("Start of intial load :")     
            delta_log_time = '1600-01-01'
            self.log.info("delta_log_time assigned default date")
        else :
            self.log.info("Start of delta log time :")    
            delta_log_time = str(delta_log_time)        
            self.log.info("Using specific date for processing.")
        self.log.info("End of extract_delta_log_time method:")
        return delta_log_time

    def write_conbase_delta(self, spark, delta_log_time = None):
        """
        This method reads timestamps from conbase delta and json file and update the delta file if 
        json time stamp is newer.
        """   
        conbase_utility = ConbaseUtility(self.log, self.config)
        delta_log_time = self.extract_delta_log_time(spark, delta_log_time,  conbase_utility.is_initial_load(spark))
        json_folders_list = self.get_json_folders(delta_log_time)  
        for json_date in json_folders_list:
            valid_from = conbase_utility.calculate_valid_from(spark, delta_log_time, json_date)     
            if self.compare_time(delta_log_time, json_date):
                self.extract_conbase_files_and_write(spark, json_date, valid_from)
            else:
                self.log.info("Delta log date is greater nothing to write:")

    def extract_conbase_files_and_write(self,spark, json_date, valid_from):
        """
        This method extract conbase files and write them to delta files.
        """
        conbase_utility = ConbaseUtility(self.log, self.config)
        prefix = self.config.get("data").get("lz").get("conbase_folder")
        initial_load = conbase_utility.is_initial_load(spark)
        conbase_current_path = prefix+json_date
        self.write_to_delta(spark, conbase_current_path, valid_from, initial_load)
