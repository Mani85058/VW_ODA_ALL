from src.common.config import Config
import os
import sys

import unittest

class TestConfig(unittest.TestCase):

    def setUp(self):
        self._yaml_path = 'staging_test.yaml'
        self._config = Config()

    def test_get(self):
        config = self._config.get_config(self._yaml_path)
        self.assertEqual(config.get("source").get("bucket") , "test-sqa-lz-inbox")
        self.assertEqual(config.get("source").get("path") , "test-core_sample/test_core_data/")
        self.assertEqual(config.get("source").get("delete_source") , False)
        self.assertEqual(config.get("source").get("file_pattern") , r'\.test_format')

if __name__ == "__main__":
    unittest.main()