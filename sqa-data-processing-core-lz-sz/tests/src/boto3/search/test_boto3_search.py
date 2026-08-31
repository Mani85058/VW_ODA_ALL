import boto3
from src.common import env
import os
import unittest
import types
from src.common.util.boto3.search import boto3_search
from src.common.config import Config
from datetime import datetime
import botocore.session
from botocore.stub import Stubber
'''
https://alexwlchan.net/2019/07/listing-s3-keys/
https://raw.githubusercontent.com/alexwlchan/alexwlchan.net/9a80d17de47b130772bb5433592e8fffd1d18118/LICENSE

Copyright (c) 2012-2019 Alex Chan

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software
is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
'''

class TestBoto3Search(unittest.TestCase):
    def setUp(self):
        self.s3 = botocore.session.get_session().create_client('s3')
        self.stubber = Stubber(self.s3)

        self.responses = {
            'IsTruncated': False,
            'Contents': [
                {
                    'Key': 'key1',
                    'LastModified': datetime(2015, 1, 1),
                    'ETag': 'string',
                    'Size': 123,
                    'StorageClass': 'STANDARD',
                    'Owner': {
                        'DisplayName': 'string',
                        'ID': 'string'
                    }
                },
                {
                    'Key': 'key2',
                    'LastModified': datetime(2015, 1, 1),
                    'ETag': 'string',
                    'Size': 123,
                    'StorageClass': 'STANDARD',
                    'Owner': {
                        'DisplayName': 'string',
                        'ID': 'string'
                    }
                },
            ],
            'Name': 'test-bucket',
            'Prefix': 'test-prefix',
            'Delimiter': 'string',
            'MaxKeys': 123,
            'CommonPrefixes': [
                {
                    'Prefix': 'string'
                },
            ],
            'EncodingType': 'url',
            'KeyCount': 123,
            'ContinuationToken': 'string',
            'NextContinuationToken': 'string',
            'StartAfter': 'string'
        }

        expected_params = {'Bucket': 'sqa-test-bucket', 'Prefix': 'test-prefix'}
        self.stubber.add_response('list_objects_v2', self.responses, expected_params)
        self.stubber.add_response('list_objects_v2', self.responses, expected_params)
        self.stubber.activate()
        service_response = self.s3.list_objects_v2(Bucket='sqa-test-bucket', Prefix ='test-prefix' )
        assert service_response == self.responses

    def test_get_matching_s3_objects(self):
        key_list = ['key1', 'key2']
        for i, objects in enumerate(boto3_search.get_matching_s3_objects(self.s3, 'sqa-test-bucket', self.responses['Prefix'])):
            self.assertEqual(objects['Key'], key_list[i], 'Wong Key returned')        
    
    def test_get_matching_s3_keys(self):
        key_list = ['key1', 'key2']
        for i, objects in enumerate(boto3_search.get_matching_s3_keys(self.s3, 'sqa-test-bucket', self.responses['Prefix'])):
            self.assertEqual(objects, key_list[i], 'Wrong Key returned')
            
if __name__ == "__main__":
    unittest.main()