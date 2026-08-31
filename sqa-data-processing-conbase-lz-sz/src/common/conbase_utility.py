import yaml
from delta.tables import DeltaTable

class ConbaseUtility:
    """
    Filters the dataframes and extract columns from dataframes.
    """
    def __init__(self, log, config):
        """
        Constructor for the ConbaseIngest class
       
        """
        self._log = log
        self._config = config 

    @staticmethod
    def get_config(file_name):
        """ 
        This method loads yaml configuration file.
        """   
        path =  "config/"+file_name        
        with open(path) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return config

    def is_delta_table(self, spark) :
        self._log.info("Start of is_delta_table method:")
        return DeltaTable.isDeltaTable(spark, self._config.get("data").get("sz").get("delta_log"))

    def is_initial_load(self, spark) :
        self._log.info("Start of is_initial_load method:")
        return not self.is_delta_table(spark)

    def calculate_valid_from(self,spark, delta_log_time, json_date):
        self._log.info("Start of calculate_valid_from method:") 
        if self.is_initial_load(spark) :
            return delta_log_time
        else :
            return json_date
   