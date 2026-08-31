from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *
import sys, os, time, random
from datetime import datetime, timedelta,date
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from src.connections.etl_connector import Connectors


spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000")\
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")\
            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")\
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")\
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
            .config('spark.sql.debug.maxToStringFields', 2000)\
            .config("spark.driver.memory", "30g")\
            .config("spark.executor.cores", "3")\
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "EsqaS3User")

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://storage.esqa.dapc-q.ocp.vwgroup.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "v8qkzonC3F65s7N58CoVEkRy")
spark.sparkContext.setSystemProperty("javax.net.ssl.trustStore", "/opt/spark/work-dir/vwca.jks")
spark.sparkContext.setSystemProperty("javax.net.ssl.trustStorePassword","vwca")

s3_glob_var = "s3a://esqa-data/gfs_poc/PROD_06162025/DIAGNOSE/DG_SLT_GLOB_VAR"

error_rec_fiter_prod_same_qs = "s3a://esqa-data/poc_partition/aggregation/error_memory/error_recovery_filter/delta/"
error_rec_fiter_prod = "sqa-oz-presentation-layer/aggregation/error_memory/error_recovery_filter/delta/"
error_recovery_filter_prt_v1 = "s3a://esqa-data/poc_partition/aggregation/error_memory/error_recovery_filter_prt_v1/delta/"
complaint_daily_prod = "s3a://esqa-data/poc_partition/Complaint/complaint_daily/delta/"
error_recovery_filter_prt_v6 = "s3a://esqa-data/poc_partition/aggregation/error_memory/error_recovery_filter_prt_v6/delta/"
Vehicle_Extended = "s3a://esqa-data/poc_partition/Vehicle/Vehicle_Extended/delta/"
complaint_daily_v1 = "s3a://esqa-data/poc_partition/Complaint/complaint_daily_v1/delta/"
vehicle_extended = 's3a://esqa-data/poc_partition/Vehicle/Vehicle_Extended/delta'

# [df_filter, df_vehicle, df_complaint]
df_filter =df = spark.read.format('delta').load(error_recovery_filter_prt_v6, header=True)
df_vehicle = spark.read.format('delta').load(Vehicle_Extended, header=True)
df_complaint = spark.read.format('delta').load(complaint_daily_prod, header=True)
vehicle_extended_df = spark.read.format('delta').load(vehicle_extended, header=True)
# df_filter.show()
# df_vehicle.show()

# # select distinct model_desc,brand, factory_name, delivery_region_de,delivery_country_de 
# df = df.select("model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_main/delta/"
# df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# distinct dfcc, model_desc,brand, factory_name, delivery_region_de,delivery_country_de
df = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
df = df.select("dfcc", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de", "ecu_name").distinct()

stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_dfcc/delta/"
df.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(stg_path)

print(f'deta table/file/object is available in {stg_path}')


# # select count (*) from (select distinct diagnosis_address || ' - ' || diagnosis_address_text AS ecu_name,model_desc,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
# df = df.select("ecu_name", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_ecu_name/delta/"
# df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# # select count (*) from (select distinct ecu_production_date_cur,diagnosis_address || ' - ' || diagnosis_address_text AS ecu_name,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
# df = df.select("ecu_production_date_cur", "ecu_name", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_ecu_production_date_cur/delta/"
# df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # # select count (*) from (select distinct factory_desc,model_desc,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)

# # # select count (*) from (select distinct ecu_production_date_cur,diagnosis_address || ' - ' || diagnosis_address_text AS ecu_name,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df = df.select("factory_desc", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_factory_desc/delta/"
# df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # # select count (*) from (select distinct hw_part_number,model_desc,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
# df = df.select("ecu_name","sw_version",  "manufacturer_name_concat", "hw_part_number", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_ecu_name_hw_sw_number/delta/"
# df.write.partitionBy("brand").format("delta").mode("overwrite").option("overwriteSchema", "true").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # # select count (*) from (select distinct id_software_version,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df_id_software_version = df.select("id_software_version", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_id_software_version/delta/"
# df_id_software_version.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # select count (*) from (select distinct manufacturer_name_concat,model_desc,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df = df.select("manufacturer_name_concat", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_manufacturer_name_concat/delta/"
# df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # select count (*) from (select distinct maturity_level,model_desc,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df_maturity_level = df.select("maturity_level", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_maturity_level/delta/"
# df_maturity_level.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# df_maturity_level = df.select("online_status", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_online_status/delta/"
# df_maturity_level.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# df_maturity_level = df.select("source_system", "online_status", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_source_system/delta/"
# df_maturity_level.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# # select count (*) from (select distinct model_year,model_desc,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df_model_year = df.select("model_year", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_model_year/delta/"
# df_model_year.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# # select count (*) from (select distinct dtc,diagnosis_address || ' - ' || diagnosis_address_text AS ecu_name,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df_pivot_event_flag = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
# df_pivot_event_flag = df_pivot_event_flag.select("dtc", "ecu_name", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()
# df_pivot_event_flag = df_pivot_event_flag.repartition("brand", "ecu_name")

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_dtc/delta/"
# df_pivot_event_flag.write.partitionBy("brand").format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# # select count (*) from (select distinct dtc,diagnosis_address || ' - ' || diagnosis_address_text AS ecu_name,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
# df = df.withColumn("dfcc_hex", upper(hex(col("dfcc").cast("int"))))
# df_pivot_event_flag = df.select("dfcc_hex", "ecu_name", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()
# df_pivot_event_flag = df_pivot_event_flag.repartition("brand", "ecu_name")

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_dfcc_hex/delta/"
# df_pivot_event_flag.write.partitionBy("brand").format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # select count (*) from (select distinct hw_version,diagnosis_address || ' - ' || diagnosis_address_text AS ecu_name,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
# # df = df.withColumn("dfcc_hex", upper(hex(col("dfcc").cast("int"))))
# df_pivot_event_flag = df.select("hw_version", "ecu_name", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()
# df_pivot_event_flag = df_pivot_event_flag.repartition("brand", "ecu_name")

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_hw_version/delta/"
# df_pivot_event_flag.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # select count (*) from (select distinct hw_version,diagnosis_address || ' - ' || diagnosis_address_text AS ecu_name,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
# # df = df.withColumn("dfcc_hex", upper(hex(col("dfcc").cast("int"))))
# df_pivot_event_flag = df.select("hw_spare_part_number", "ecu_name", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()
# df_pivot_event_flag = df_pivot_event_flag.repartition("brand", "ecu_name")

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_hw_spare_part_number/delta/"
# df_pivot_event_flag.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# # select count (*) from (select distinct pivot_event_flag,diagnosis_address || ' - ' || diagnosis_address_text AS ecu_name,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df_pivot_event_flag = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
# df_pivot_event_flag = df_pivot_event_flag.select("pivot_event_flag", "ecu_name", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_pivot_event_flag/delta/"
# df_pivot_event_flag.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # select count (*) from (select distinct vehicle_model_de, factory_desc, model_desc,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df_model_year = df.select("vehicle_model_de", "factory_desc", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_Modell/delta/"
# df_model_year.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # select count (*) from (select distinct sw_version,model_desc,diagnosis_address || ' - ' || diagnosis_address_text AS ecu_name,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.error_recovery_filter_prt_v6)
# df = df.withColumn("ecu_name",concat(df["diagnosis_address"], lit(" - "), df["diagnosis_address_text"]))
# df_model_year = df.select("ecu_name", "sw_version", "model_desc","brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_sw_version/delta/"
# df_model_year.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# df_vehicle = spark.table("esqa.vehicle_extended")
# df_filter = spark.table("esqa.error_recovery_filter")

# er_filtered = df_filter.select("model_key_3", "model_desc").distinct()
# joined_df = df_vehicle.join(er_filtered, on="model_key_3", how="inner")
# final_df = joined_df.select(
#     "brand",
#     "model_key_3",
#     col("zp8_date").cast("string").alias("zp8_date_start"),
#     col("zp8_date").cast("string").alias("zp8_date_end"),
#     "model_desc",
#     "factory_name",
#     "delivery_region_de",
#     "delivery_country_de"
# ).distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_zp8_date/delta/"
# final_df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # select count(*) from (select DISTINCT workshop_description_short, brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.complaint_daily_prod) c
# # 	join (select model_desc, model_key_3 from esqa_det_oz.error_recovery_filter_prt_v6) e on c.model_key_3=e.model_key_3)

# # [df_filter, df_vehicle, df_complaint]
# er_filtered = df_filter.select("model_key_3", "model_desc").distinct()
# joined_df = df_complaint.join(er_filtered, on="model_key_3", how="inner")
# joined_df = joined_df.select ("workshop_description_short","model_desc", "brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()
# joined_df = joined_df.repartition("brand", "model_desc")

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_workshop_description_short/delta/"
# joined_df.write.partitionBy("brand").format("delta").mode("overwrite").option("overwriteSchema", "true").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# # "select count(*) from (select DISTINCT repair_shop_fault_type_group, brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.complaint_daily_prod c
# 	# join (select model_desc, model_key_3 from esqa_det_oz.error_recovery_filter_prt_v6) e on c.model_key_3=e.model_key_3)"

# er_filtered = df_filter.select("model_key_3", "model_desc").distinct()
# joined_df = df_complaint.join(er_filtered, on="model_key_3", how="inner")
# joined_df = joined_df.select ("repair_shop_fault_type_group","model_desc", "brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_repair_shop_fault_type_group/delta/"
# joined_df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# # select count(*) from (select distinct CASE WHEN date_diff('month', cd.delivery_date, complaint_date) <= 48
# #     THEN CONCAT('MIS ', CAST(date_diff('month', cd.delivery_date, complaint_date) as varchar))
# #     ELSE NULL 
# #   END as mis_label,brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.complaint_daily_prod cd)
# #  -- 95144


# er_filtered = df_filter.select("model_key_3", "model_desc").distinct()
# joined_df = df_complaint.join(er_filtered, on="model_key_3", how="inner")
# df_complaint = joined_df.withColumn("months_diff", floor(months_between(col("complaint_date"), col("delivery_date"))))
# df_complaint = df_complaint.withColumn("mis_label",when(col("months_diff") <= 48, concat(lit("MIS "), col("months_diff").cast("string"))).otherwise(None))
# df_complaint = df_complaint.select("mis_label", "months_diff", "brand","model_desc", "factory_name", "delivery_region_de", "delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_mis_label/delta/"
# df_complaint.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# # "select count(*) from (select DISTINCT repair_shop_fault_type, brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.complaint_daily_prod c
# # 	join (select model_desc, model_key_3 from esqa_det_oz.error_recovery_filter_prt_v6) e on c.model_key_3=e.model_key_3)"

# er_filtered = df.select("model_key_3", "model_desc").distinct()
# joined_df = df_complaint.join(er_filtered, on="model_key_3", how="inner")
# joined_df = joined_df.select ("repair_shop_fault_type","model_desc", "brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_repair_shop_fault_type/delta/"
# joined_df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # "select count(*) from (select DISTINCT repair_shop_fault_object_main_group brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.complaint_daily_prod) c
# # 	join (select model_desc, model_key_3 from esqa_det_oz.error_recovery_filter_prt_v6) e on c.model_key_3=e.model_key_3)"

# er_filtered = df_filter.select("model_key_3", "model_desc").distinct()
# joined_df = df_complaint.join(er_filtered, on="model_key_3", how="inner")
# joined_df = joined_df.select ("repair_shop_fault_object_main_group","model_desc", "brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_repair_shop_fault_object_main_group/delta/"
# joined_df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # "select count(*) from (select DISTINCT repair_shop_fault_object_detail, brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.complaint_daily_prod c
# # 	join (select model_desc, model_key_3 from esqa_det_oz.error_recovery_filter_prt_v6) e on c.model_key_3=e.model_key_3)"

# er_filtered = df_filter.select("model_key_3", "model_desc").distinct()
# joined_df = df_complaint.join(er_filtered, on="model_key_3", how="inner")
# joined_df = joined_df.select ("repair_shop_fault_object_detail","model_desc", "brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_repair_shop_fault_object_detail/delta/"
# joined_df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')


# # select count(*) from (select DISTINCT customer_complaint_text, brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.complaint_daily_prod c
# # 	join (select model_desc, model_key_3 from esqa_det_oz.error_recovery_filter_prt_v6) e on c.model_key_3=e.model_key_3)
# er_filtered = df_filter.select("model_key_3", "model_desc").distinct()
# joined_df = df_complaint.join(er_filtered, on="model_key_3", how="inner")
# joined_df = joined_df.select ("customer_complaint_text","model_desc", "brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()
# joined_df = joined_df.repartition("brand", "model_desc")

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_customer_complaint_text/delta/"
# joined_df.write.partitionBy("brand").format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')



# # select count(*) from (select DISTINCT customer_fault_object_main_group, brand, factory_name, delivery_region_de,delivery_country_de from esqa_det_oz.complaint_daily_prod) c
# # 	join (select model_desc, model_key_3 from esqa_det_oz.error_recovery_filter_prt_v6) e on c.model_key_3=e.model_key_3)

# er_filtered = df_filter.select("model_key_3", "model_desc").distinct()
# joined_df = df_complaint.join(er_filtered, on="model_key_3", how="inner")
# joined_df = joined_df.select ("customer_fault_object_main_group","model_desc", "brand", "factory_name", "delivery_region_de","delivery_country_de").distinct()

# stg_path = "s3a://esqa-data/poc_partition/aggregation/error_filter/error_recovery_by_customer_fault_object_main_group/delta/"
# joined_df.coalesce(1).write.format("delta").mode("overwrite").save(stg_path)

# print(f'deta table/file/object is available in {stg_path}')

# vehicle_extended_df






























# ===========================================================================================================================================================================================================
# ===========================================================================================================================================================================================================
# ===========================================================================================================================================================================================================
# ===========================================================================================================================================================================================================
# ===========================================================================================================================================================================================================
# ===========================================================================================================================================================================================================
# ===========================================================================================================================================================================================================
            #  case
            #      when newer_baselines = 0 then true
            #      when newer_baselines >0 then false
            #  end as valid














# spark-submit error_filter_sep_tbl.py