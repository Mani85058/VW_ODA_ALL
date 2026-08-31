from pyspark.sql import SparkSession , SQLContext

class SparkApplication:
    """
    Creates spark application, read and write files.
    """
    def create_spark_app(self):
        """
        Creates spark application.
        """
        return SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").getOrCreate()

    def read_delta_files(self, spark, filepath):
        """
        Read delta files from given path.
        """        
        return spark.read.format("delta").load(filepath)

    def read_csv_files(self, spark, filepath):
        """
        Read csv files from given path.
        """
        return spark.read.option("header", "true")\
        .option("timestampFormat","yyyy-MM-dd")\
        .option("inferSchema", "true")\
        .format("csv").load(filepath)
    
    def read_csv_rvs(self, spark, config_path, config_header_list):
        """
        Read interface csv files from given path.
        """
        # Read CSV without header
        df = spark.read.format("csv") \
            .option("delimiter", ";") \
            .option("header", "false") \
            .load(config_path)
        
        # Map required config headers to actual _cN column names
        # Example: config_header_list[0] -> alias for _c0, etc.
        select_exprs = [
            f"_c{i} as {col_name}" for i, col_name in enumerate(config_header_list)
        ]
        
        # Select only required columns
        df = df.selectExpr(*select_exprs)
        
        return df
    
    def read_parquet_files(self, spark, filepath):
        """
        Read delta files from given path.
        """
        return spark.read.parquet(filepath)
    
    def write_parquet_files(self, dataframe, filepath):
        """
        Write dataframe to the given path.
        """     
        return dataframe.write.mode("overwrite").parquet(filepath)
    
    def write_parquet_files_num_partition(self, dataframe, filepath, partitions):
        """
        Repartition and write dataframe to the given path.
        """
        return dataframe.repartition(partitions).write.mode("overwrite").parquet(filepath)
    
    def write_parquet_files_column_partition(self, dataframe, filepath, *partition_columns):
        """
        Write dataframe with partion by mentioned columns to the given path.
        """
        return dataframe.write.partitionBy(partition_columns).mode("overwrite").parquet(filepath)

    def read_files(self, spark, filepath, format):
        """
        Read files from given path depending on the format.
        """       
        if format == "delta":
            return self.read_delta_files(spark, filepath)
        if format == "csv" :
            return self.read_csv_files(spark, filepath)
        if format == "parquet":
            return self.read_parquet_files(spark, filepath)
        


    
        
