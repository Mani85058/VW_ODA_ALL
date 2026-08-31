class ConfigFileNotFoundError(Exception):
    """
    Raise when no configuration file is found. 
    """
    pass

class SourcePathAndColumn(Exception):
    """
    Raise when source path or columns are wrong. 
    """
    pass 

class SoureNameError(Exception):
    """
    Raise when wrong configuration passed for source files. 
    """
    pass 

class MissingUsernameOrPassword(Exception):
    """
    Raise when either user name or password is missing/wrong. 
    """
    pass

class EtlException(Exception):
    """
    Raise when an exception occurs in etl.
    """
    pass             

class WrongTimeFormat(Exception):
    """
    Raise when the user entered wrong type of time format
    """
    pass

class BotoCoreException(Exception):
    """
    Raise when the botocore could not connect as requested 
    """
    pass

class NoFilesFoundException(Exception):
    """
    Raise when the S3 bucket has no files
    """
    pass

