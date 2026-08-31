from src.common import env
import os
import sys
import unittest


class TestEnv(unittest.TestCase):

    def test_get(self):
        
        #test 1
        with self.assertRaises(KeyError):
            env.get("TARGET_WRITE_MODEfgbhfdghfg")
        
        #test 2 step 1 prepare data
        os.environ["TARGET_WRITE_MODEfgbhfdghfg"]="eee"
        
        # step 2 function testing 
        self.assertEqual(env.get("TARGET_WRITE_MODEfgbhfdghfg"), "eee" ,"TARGET_WRITE_MODEfgbhfdghfg must be dfvdf now")
        
        # step 3 cleanup
        del os.environ['TARGET_WRITE_MODEfgbhfdghfg']
    
    
    def test_get_lower_case(self):
        
        # test environment get with yaml lower case 
        os.environ["TAB_YAML_PATH"]="test_value"
        self.assertEqual(env.get("tab_yaml_path"), "test_value")


if __name__ == "__main__":

    unittest.main()