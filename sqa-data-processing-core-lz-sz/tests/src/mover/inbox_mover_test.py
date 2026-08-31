import unittest
import os
import datetime
import boto3, botocore

from unittest import mock
from unittest.mock import patch
from botocore.stub import Stubber
from unittest.mock import Mock

from botocore.config import Config as BotoConfig
from src.common.config import Config
from src.inbox_to_landing.inbox_mover import InboxMover
from src.common.util.core_utils import CoreUtils
from src.common.log.log import Logger as Log

import urllib3
urllib3.disable_warnings()


class test_mover(unittest.TestCase):
    
    def setUp(self):
        self.aws_files = [
            "tests/files/QCZ.N0LKGZ.SQAMODPROD.D20221231.Test.csv"
        ]

    @classmethod
    def setUpClass(cls):
        cls.env_patcher = mock.patch.dict(os.environ, {
            "APP_NAME": "aws-fleet",
            "var_log_job_object": "test_log_object",
            "TMP_FOLDER": "/tmp/cdis-aws/",
            'source': "",
            "target": "" ,
            "log": "" ,
            "SOURCE_S3_MILESTONE": "",
            "SOURCE_S3_HOURS_BACK": "4",
            #
            #   proxy
            #
            "PROXY_USER": "",
            "PROXY_PASSWORD": "",
            "PROXY_URL": "",
            "PROXY_PORT": "",
           
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "S3_ENDPOINT_URL": ""
        })
        cls.env_patcher.start()
        super().setUpClass()

    def register_response(self):
        self.response = {
            'Produkt': 'string',
            'Beschreibung': 'string',
            'Produkttyp': 'string',
            'VersionId': 'string',
            'Vertriebliche Klasse': 'string'
        }
        expected_params = {'Body': stub.ANY, 'Bucket':self.log.log_conf.get('LOG_S3_BUCKET') ,'Key':self.log.log_conf.get('LOG_S3_PREFIX')+ self.log.log_conf.get('LOG_FILE_NAME')}
        self.stubber.add_response('put_object', self.response, expected_params)

    @mock.patch('src.common.util.core_utils.CoreUtils.move_files')
    def test_main(self, copy_files_mocked):

        def mockup_move_files (*args, **kwargs):
            return []

        moved_files_mocked = mockup_move_files

        self.assertTrue(moved_files_mocked, "main must return True")

    @mock.patch('src.common.util.core_utils.CoreUtils.get_boto3client')
    def test_get_source_client_s3(self, get_boto3client_s3_mocked):

        def mockup_get_boto3client_s3 (url, user, password):
            return mock.Mock()

        get_boto3client_s3_mocked = mockup_get_boto3client_s3

        self.assertIsNotNone(get_boto3client_s3_mocked, "get_source_client must return the s3 client")
    
    @mock.patch('src.common.util.core_utils.CoreUtils.get_boto3resource')
    def test_get_boto3resource(self, get_boto3resource_mocked):

        def mockup_get_boto3resource (url, user, password):
            return mock.Mock()

        get_boto3resource_mocked = mockup_get_boto3resource

        self.assertIsNotNone(get_boto3resource_mocked, "get_boto3resource must return the s3 client")
    
    def test_show_list(self):
        list = [1,2,3,4,5]
        log = Mock(spec=Log)
        core_utls = CoreUtils(log)
        test_output = core_utls.show_list(list)
        self.assertEqual(test_output.get("status"), "OK", "List status should be OK")

    @mock.patch('src.common.util.core_utils.CoreUtils.s3_is_file_exist')
    def test_s3_is_file_exist(self, s3_is_file_exist_mocked):
        
        def mockup_s3_is_file_exist(s3_client, bucket, key):
            return mock.Mock()
        
        s3_is_file_exist_mocked = mockup_s3_is_file_exist
        
        self.assertTrue(s3_is_file_exist_mocked, "s3_is_file_exist must return True")

    @mock.patch('src.common.util.core_utils.CoreUtils.file_in_list')
    def test_s3_is_file_exist(self, file_in_list_mocked):
        
        def mockup_file_in_list(filename, list):
            return mock.Mock()
        
        file_in_list_mocked = mockup_file_in_list
        
        self.assertTrue(file_in_list_mocked, "file_in_list must return True")
        
    @mock.patch('src.common.util.core_utils.CoreUtils.get_duplicate_version')
    def test_get_duplicate_version(self, mockup_get_duplicate_version):
        
        def mockup_get_duplicate_version(target_duplicates_list, filename):
            return mock.Mock()

        duplicate_version_mocked = mockup_get_duplicate_version

        self.assertIsNotNone(duplicate_version_mocked, "get_duplicate_version must return the version")

    @mock.patch('src.common.util.core_utils.CoreUtils.move_file')
    def test_move_file(self, copy_file_mocked):

        def mockup_move_file (s3_client, old_bucket_name, old_object_name, new_bucket_name, new_object_name, delete_source_files, log):
            return True

        copy_file_mocked = mockup_move_file

        self.assertTrue(copy_file_mocked, "move_file must return True")

    @mock.patch('src.common.util.core_utils.CoreUtils.move_files')
    def test_move_files(self, moved_file_mocked):

        def mockup_copy_file (s3_client, source_list, target_list, target_duplicates_list, target_base_path, target_duplicates_path_base, duplicates_strategy, delete_source_files, log):
            return {"status": "OK", "total": file_count, "duplicates": duplicates}

        moved_file_mocked = mockup_copy_file

        def get_boto3resource(self, get_boto3resource_mocked):
            return self.aws_files
        log = Mock(spec=Log)
        core_utls = CoreUtils(log)
        copied_files_list = core_utls.move_files(
                #
                f_get_list = get_boto3resource,
                #
                s3_source_client = None,
                s3_source_bucket = "",
                s3_source_path = "",
                #
                s3_target_client = None,
                s3_target_bucket = "",
                s3_target_path = "",
                #
                tmp_folder = ""
            )
        self.assertIsNotNone(copied_files_list, "all files must be copied")
    
    def test_list_filtered (self):
        test_s3_files = [{"target_file":"1a_test.csv"},{"target_file":"2a_test.csv"},{"target_file":"3a_test.json"},{"target_file":"dummy.txt"}]

        log = Mock(spec=Log)
        core_utls = CoreUtils(log)
        test_output = core_utls.filter_file_list(
                        list = test_s3_files,
                        pattern = ".csv")

        self.assertEqual(len(test_output), 2, "List filtered have to collect just csv files")

    def test_inbox_mover(self):
        log = Mock(spec=Log)
        core_utls = CoreUtils(log)
        config = Config.get_config("staging_test.yaml")
        inbx_mvr = Mock(spec=InboxMover(log, config))
        main_test = Mock(spec=inbx_mvr.main())
        self.assertIsNotNone(main_test, "main works")

if __name__ == '__main__':
    unittest.main()