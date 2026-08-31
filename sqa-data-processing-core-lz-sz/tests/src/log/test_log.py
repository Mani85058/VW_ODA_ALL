from src.common.log.log import Logger
from src.common import env
import sys
import unittest
from unittest import TestCase, mock
from botocore.stub import Stubber
from botocore import stub
import json
import datetime
import time
import os

class TestLog(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.env_patcher = mock.patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "ABC", "AWS_SECRET_ACCESS_KEY": "XYZ", "S3_ENDPOINT_URL": "https://mopitzs3end.com"})
        cls.env_patcher.start()
        super().setUpClass()

    def setUp(self):
        self.log = Logger(1, "test", "core", "coretest", "coretest")
        self.stubber = Stubber(self.log.boto3client)
        self.stubber.activate()
    
    def register_response(self):
        self.response = {
            'Expiration': 'string',
            'ETag': 'string',
            'ServerSideEncryption': 'AES256',
            'VersionId': 'string',
            'SSECustomerAlgorithm': 'string',
            'SSECustomerKeyMD5': 'string',
            'SSEKMSKeyId': 'string',
            'SSEKMSEncryptionContext': 'string',
            'BucketKeyEnabled': False,
            'RequestCharged': 'requester'
        }
        expected_params = {'Body': stub.ANY, 'Bucket':self.log.log_conf.get('LOG_S3_BUCKET') ,'Key':self.log.log_conf.get('SPARK_APP_NAME')+ self.log.log_conf.get('JOB_OBJECT')}
        self.stubber.add_response('put_object', self.response, expected_params)

    def raise_check(self, msg):
        raise Exception(msg)

    def test_raise_check(self):
        msg = 'Error thrown as expected'
        with self.assertRaises(Exception):
            self.raise_check(msg)

    def test_set_flush_limit(self):
        self.register_response()
        self.log.set_flush_limit(3)
        self.assertEqual(self.log.flush_limit, 3, 'Wrong flush_limit!')
    
    def test_add_spark_id(self):
        self.register_response()
        class SparkContext:
            applicationId = 5
        class Spark:
            sparkContext = SparkContext()
        self.log.add_spark_id(Spark())
        print(self.log.log_conf['SPARK_APP_ID'])
        self.assertEqual(self.log.log_conf['SPARK_APP_ID'], 5, "Wrong Spark Application ID!!!")
        
    """def main():
    ### Init variables for testing

        ## check availability of environment variables needed for LOG to work
        var_env_log_s3_endpoint_url          = env.get('S3_ENDPOINT_URL') #QS# export S3_ENDPOINT_URL='https://storage.esqa.dapc-q.ocp.vwgroup.com/'
        var_env_log_AWS_ACCESS_KEY_ID        = env.get('AWS_ACCESS_KEY_ID')
        var_env_log_AWS_SECRET_ACCESS_KEY    = env.get('AWS_SECRET_ACCESS_KEY')
        #var_env_log_s3_bucket                = env.get('LOG_S3_BUCKET') # default value LOG_S3_BUCKET='sqa-job-logs' if env variable not set
        #var_env_log_level                    = int(env.get('LOG_LEVEL')) # default value 1 if env variable not set

        # Log Levels: 0 debug, 1 info, 2 warn, 3 error, 4 critical
        #             only write to file if log_level of message >= log_level of ENV Variable LOG_LEVEL
        #             only uses print function if level is set to debug

        #every spark app should have a name
        var_spark_app_name              = 'SPARK_APP_TEST_LOG'

        # setup logging (write via boto3)
        # var_log_s3_prefix               = config.get('LOG_S3_PREFIX') # see default value in Log class if value None
        # var_log_job_object              = var_target_table_name # example from main
        #var_log_s3_prefix               = 'lz-sz-application-logs/testlog/'
        var_log_s3_prefix               = ''
        var_job_object                  = 'log_test_object_1'
        with Log(job_object=var_job_object,spark_app_name=var_spark_app_name, log_s3_prefix=var_log_s3_prefix) as joblog:

            # start spark
            spark = SparkSession.builder\
                .appName(var_spark_app_name)\
                .getOrCreate()

            # add spark app id information (after SparkSession.builder)
            joblog.add_spark_id(spark)    

            ##test logging functionality
            joblog.info('info test message 1')
            joblog.debug('debug test message 2')
            joblog.warn('warn test message 3')
            joblog.error('error test message 4')
            joblog.critical('critical test message 5')

            #test exception handling in application log
            
            #raise_test('ABC')"""
        

    if __name__ == "__main__":
        unittest.main()