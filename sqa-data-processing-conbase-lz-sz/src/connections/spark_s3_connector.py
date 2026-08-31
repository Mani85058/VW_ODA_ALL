from pyspark import SparkConf, SparkContext

import os

class Connectors:

    @staticmethod
    def s3connect(spark):        
        spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.environ['S3_ENDPOINT_URL'])    