from src.common import env

class s3conn:
    """
    This module connects s3 with spark
    """
    @staticmethod
    def s3connect(config, spark):

        spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", env.get('AWS_ACCESS_KEY_ID'))
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", env.get('AWS_SECRET_ACCESS_KEY'))
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", env.get('S3_ENDPOINT_URL'))

   