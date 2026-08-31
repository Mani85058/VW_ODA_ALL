import yaml
class Config:
    """
    Filters the dataframes and extract columns from dataframes.
    """
    @staticmethod
    def get_config(file_name):
        """ 
        This method loads yaml configuration file.
        """   
        path =  "config/"+file_name        
        with open(path) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return config

    