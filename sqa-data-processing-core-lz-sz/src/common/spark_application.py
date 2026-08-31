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
    
    def read_files(self, spark, filepath, format):
        """
        Read files from given path depending on the format.
        """       
        if format == "delta":
            return self.read_delta_files(spark, filepath)
        if format == "csv" :
            return self.read_csv_files(spark, filepath)
        