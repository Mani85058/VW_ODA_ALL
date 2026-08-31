import re
import os

from datetime import datetime, timezone, timedelta, date
from dateutil.relativedelta import relativedelta

from src.connections.s3.s3_operations import S3Operations
from src.common.sparkdelta.delta_operations import DeltaOperations

from delta.tables import DeltaTable
from pyspark.sql.functions import explode, current_date
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DateType
import pyspark.sql.functions as F

from src.common.log.log import Logger
from src.common.util.core_utils import CoreUtils

from src.common.exceptions import  MissingUsernameOrPassword, EtlException, NoFilesFoundException
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

class LzSzCore:
    """
    This class converts new raw core file from csv to delta format .
    """

    def __init__(self, log, config, spark):
        self.log = log
        self.config = config 
        self._today = datetime.date(datetime.now(timezone.utc))
    
    def normalization_operation(self, spark, spark_app, core_df):
        """
        This method provides the distribution of the years in the core data and
        brings it to the desired format
        Note: it is not required and not used according to the business requirements, 
        but it was kept cause it might be needed in the future
        """
        self.log.info("Start of normalization_operation method:")
        core_utils = CoreUtils(self.log)
        self.log.info("Starting of normalization")
        series_and_year = core_utils.extract_series_and_years_of_model(core_df)
        self.log.info("Extracting years and series successful")
        self.log.info("End of normalization_operation method")
        return series_and_year

    def write_to_delta(self, spark, spark_app, s3_opt, delta_opt, core, is_initial_load):
        """
        Execute the write_to_delta of core data and valid_from_date in five steps:
            Step 1 - Checks the null materials 
            Step 2 - Taking only not null material ones with filter_null_materials
            Step 3 - Extract the latest uploaded core csv file with read_and_sort_core_files
            Step 4 - Writes the delta files with controlled_writing
        """

        self.log.info("Start of write_to_delta method:")        
        
        for data_table in core:
            s3_data = spark.read.option("header","true").option("delimiter", ";").csv(self.config.get("data").get("lz").get("landing_zone")+data_table)\
            .withColumn("Load_Date", F.lit(date.today()))\
            .withColumn("KSU_LOESCH_DATUM", F.lit(date.today() + relativedelta(years = +int(self.config.get("ksu").get("period_years")),
            months = +int(self.config.get("ksu").get("period_months")), days = +int(self.config.get("ksu").get("period_days")))))
            if self.table_controller(spark, s3_data) == "modkpb":
                data_modkpb = s3_data.select("MODEL","SALES_CLASS","PRODUCT","KPB","Load_Date","KSU_LOESCH_DATUM")\
                .toDF("Model","Sales_Class","Product","KPB","Load_Date","KSU_LOESCH_DATUM")
                to_update = delta_opt.get_update_flag(is_initial_load, "delta_log_sqamodkpb")
                self.controlled_writing_for_sqamodkpb(spark, data_modkpb, to_update)
            elif self.table_controller(spark, s3_data) == "modprod":
                data_modprod = s3_data.select("MODEL","MODEL_YEAR","SALES_CLASS","PRODUCT","Load_Date","KSU_LOESCH_DATUM")\
                .toDF("Model","Model_Year","Sales_Class","Product","Load_Date","KSU_LOESCH_DATUM")
                to_update = delta_opt.get_update_flag(is_initial_load, "delta_log_sqamodprod")     
                self.controlled_writing_for_sqamodprod(spark, data_modprod, to_update)
            else:
                self.log.error(f"{data_table} format is wrong, it will not proccessed")

    def table_controller(self, spark, table):
        """
        This method performs a structure based control and categorizes the data in the inbox bucket.
        """

        self.log.info("Start of table_controller method:")
        self.log.info("Start of table_controller for modprod:")
        schema_for_modprod = StructType([\
        StructField("MODEL",StringType(),True),
        StructField("MODEL_YEAR",StringType(),True),
        StructField("SALES_CLASS",StringType(),True),
        StructField("PRODUCT",StringType(),True),
        StructField("Load_Date",DateType(),False),
        StructField("KSU_LOESCH_DATUM",DateType(),False)])
        self.log.info("schema_for_modprod created")

        self.log.info("Start of table_controller for modkpb:")
        schema_for_modkpb = StructType([\
        StructField("MODEL",StringType(),True),
        StructField("SALES_CLASS",StringType(),True),
        StructField("PRODUCT",StringType(),True),
        StructField("KPB",StringType(),True),
        StructField("Load_Date",DateType(),False),
        StructField("KSU_LOESCH_DATUM",DateType(),False)])
        self.log.info("schema_for_modkpb created")
        
        self.log.info("Start of table type control:")
        if table.schema == schema_for_modprod:
            self.log.info("Table type is sqamodprod:")
            self.log.info(f"{table} structure is in a format suitable for the modprod structure")
            return "modprod"
        elif table.schema == schema_for_modkpb:
            self.log.info("Table type is sqamodkpb:")
            self.log.info(f"{table} structure is in a format suitable for the modkpb structure")
            return "modkpb"
        else:
            self.log.info("Table type is unknown")
            self.log.error(f"{table} structure does not have desired structure")
            return self.log.error(f"{table} schema is wrong")
           
    def controlled_writing_for_sqamodprod(self, spark, core_attributes_df, to_update):
        """
        Performs the execute operation by checking the number of files contained in the core data.
        """
        self.log.info("Start of controlled_writing method:")
        if not to_update :
            self.log.info("Start of initial load:")
            self.log.info("sqamodprod delta file is writing")
            core_attributes_df.write\
            .partitionBy("Load_Date")\
            .format(self.config.get("data").get("sz").get("format")).mode("append")\
            .save(self.config.get("data").get("sz").get("delta_log_sqamodprod"))
            self.log.info("Write is finished")
            self.log.info("End of initial load")
        else: 
            delta_table = DeltaTable.forPath(spark, self.config.get("data").get("sz").get("delta_log_sqamodprod"))
            # WIP: new columns needs to be added after normalization 
            delta_table.alias("events").merge(
                core_attributes_df.alias("updates"),
                "events.Load_Date = updates.Load_Date AND events.KSU_LOESCH_DATUM = updates.KSU_LOESCH_DATUM AND events.Model = updates.Model AND events.Model_Year = updates.Model_Year AND events.Sales_Class = updates.Sales_Class AND events.Product = updates.Product" )\
                .whenNotMatchedInsert(values =
                {
                "Model": "updates.Model",
                "Model_Year": "updates.Model_Year",
                "Sales_Class": "updates.Sales_Class",
                "Product": "updates.Product",     
                "KSU_LOESCH_DATUM": "updates.KSU_LOESCH_DATUM",
                "Load_Date": "updates.Load_Date"
                }
            ) \
            .execute()
        self.log.info("End of controlled_writing method:")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        

    def controlled_writing_for_sqamodkpb(self, spark, core_attributes_df, to_update):
        """
        Performs the execute operation by checking the number of files contained in the core data.
        """
        self.log.info("Start of controlled_writing method:")
        if not to_update :
            self.log.info("Start of initial load:")
            self.log.info("sqamodkpb delta file is writing")
            core_attributes_df.write\
            .partitionBy("Load_Date")\
            .format(self.config.get("data").get("sz").get("format")).mode("append")\
            .save(self.config.get("data").get("sz").get("delta_log_sqamodkpb"))
            self.log.info("Write is finished")
            self.log.info("End of initial load:")
        else:
            delta_table = DeltaTable.forPath(spark, self.config.get("data").get("sz").get("delta_log_sqamodkpb"))
            # WIP: new columns needs to be added after normalization   
            delta_table.alias("events").merge(
                core_attributes_df.alias("updates"),
                "events.Load_Date = updates.Load_Date AND events.KSU_LOESCH_DATUM = updates.KSU_LOESCH_DATUM AND events.Model = updates.Model AND events.Sales_Class = updates.Sales_Class AND events.Product = updates.Product AND events.KPB = updates.KPB")\
                .whenNotMatchedInsert(values =
                {
                "Model": "updates.Model",
                "Sales_Class": "updates.Sales_Class",
                "Product": "updates.Product",   
                "KPB": "updates.KPB",
                "KSU_LOESCH_DATUM": "updates.KSU_LOESCH_DATUM",
                "Load_Date": "updates.Load_Date"
                }
            ) \
            .execute()
        self.log.info("End of controlled_writing method:") 
   
    def compare_time(self, delta_log_time, csv_time):
        """
        This methods compares the date between core deltalog file and core csv file
        and return true if core csv file is newer than deltalog file.
        """
        self.log.info("Start of compare_time method:")
        delta = self.convert_time_string(delta_log_time)
        self.log.info("Delta time revealed")
        csv = self.convert_time_string(csv_time)
        self.log.info("Core csv time revealed")
        self.log.info("End of collecting csv and delta time")
        self.log.info("Start of csv and delta time comparison")
        if csv > delta :            
            self.log.info("End of compare time method csv is newer than delta")
            self.log.info("CSV date is greater - continue to writing")
            return True
        else:            
            self.log.info("End of compare time method: csv is older than delta")
            return False

    def convert_time_string(self, time_string: str):
        """
        This method converts a time string of format '%Y-%m-%d' to datetime.
        """
        self.log.info("Converting time string to datetime.")
        self.log.info("This method will convert delta and csv time information to datetime")
        time_converted = datetime.strptime(time_string, "%Y-%m-%d")
        self.log.info("Coverted time string to datetime successfully.")

        return time_converted 

    def write_core_delta(self, spark, spark_app, delta_log_time = None):
        """
        This method reads timestamps from core delta and csv file and update the delta file if 
        csv time stamp is newer.
        """
        
        self.log.info("Start of write_core_delta method:") 
        delta_opt = DeltaOperations(spark, spark_app, self.config, self.log)
        s3_opt = S3Operations(spark, spark_app, self.config, self.log)
        delta_log_time, is_inital_load = delta_opt.get_delta_log(delta_log_time)
        csv_folders_list, core_files_count = s3_opt.get_csv_folders(delta_log_time)
        
        if core_files_count < 1:
            raise NoFilesFoundException("No core data files found in landing zone") 
        else:
            for csv_date in csv_folders_list: 
                if self.compare_time(delta_log_time, csv_date):
                    prefix  = self.config.get("data").get("lz").get("core")+f"{csv_date}/"
                    list_of_new_files = s3_opt.latest_files_collector(s3_opt.landing_zone_connector(),prefix, delta_log_time)
                    self.write_to_delta(spark, spark_app, s3_opt, delta_opt, list_of_new_files,is_inital_load)
                else:
                    self.log.info("Delta log date is greater - nothing to write")
        self.log.info("End of write_core_delta method:")