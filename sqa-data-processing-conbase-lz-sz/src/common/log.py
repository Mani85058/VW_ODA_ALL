import datetime, json, time, tempfile, traceback
from src.connections.s3 import S3Connector


class Logger:
    """
    Application Logging for spark driver pod. 
    Provides high level overview of the status without all the spark log output in the way
    This is not intended to be used by the spark executors or for the actual spark logs
    """

    def __init__(self, app_log_level, log_bucket_name, log_folder_name, spark_app_name = 'undefined' , job_object = 'undefined'):
        self.flush_counter = 0
        # after how many lines logged is the log pushed to s3
        self.flush_limit = 1
        # 'inmemory' or 'tmpfile' for holding log before pushing to S3
        self.internal_log_mode = 'inmemory'
        self.log_level_desc = ['Debug','Info','Warn','Error','Critical']
        #all entries of log stored here temporarily
        self.inmemory_log = None
        self.app_log_level = app_log_level        
        log_s3_prefix = log_folder_name + "/" + job_object + "/"
        #configuration for Log functionality
        self.log_conf = {'CONFIG_DESCRIPTION': 'Application Log Config PySpark',
        'JOB_OBJECT': job_object,
        'SPARK_APP_NAME' : spark_app_name,  #getting the spark application name
        'INIT_TIMESTAMP_STR': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S-%f'), #getting the timestamp of the job start
        'LOG_S3_BUCKET': log_bucket_name,
        'LOG_S3_PREFIX': log_s3_prefix, 
        'APP_LOG_LEVEL': self.app_log_level,
        'LOG_FLUSH_LIMIT': self.flush_limit,
        'LOG_TMP_FOLDER': "/temporary/"
        }
        #creating the log file name with json extension
        self.log_conf['LOG_FILE_NAME'] = self.log_conf.get('INIT_TIMESTAMP_STR') +\
        '_' + self.log_conf.get('SPARK_APP_NAME') +\
        '_' + self.log_conf.get('JOB_OBJECT') +'.json' 
        self.tmp_log_file_path = self.log_conf.get('LOG_TMP_FOLDER')+self.log_conf.get('LOG_FILE_NAME')                                
        #creating boto3 s3 client so log (write to s3) can be used 
        self.boto3client = S3Connector.create_boto3_client() 

    def set_flush_limit(self, flush_limit):
        """
        This method changes flush limit and takes effect on next log entry.
        """

        self.debug('Changing flush limit from '+str(self.flush_limit)+' to '+str(flush_limit))
        self.flush_limit = flush_limit
        
    
    def add_spark_id(self, spark):
        """
        This method adds spark object and information to logger (can only be set after spark is set).
        """

        self.log_conf['SPARK_APP_ID'] = spark.sparkContext.applicationId #getting the spark application ID
        self.debug('Spark Application Id provided') 

    def debug(self, log_message):
        """
        This method adds log message on debug level.
        """

        self.log(log_message = log_message, log_level=0, write_mode ='buffered') 

    def info(self, log_message):
        """
        This method adds log message on info level.
        """

        self.log(log_message = log_message, log_level=1, write_mode ='buffered')

    def warn(self, log_message):
        """
        This method adds log message on warn level.
        """

        self.log(log_message = log_message, log_level=2, write_mode ='immediate')

    def error(self, log_message): 
        """
        This method adds log message on error level.
        """ 

        self.log(log_message = log_message, log_level=3, write_mode ='immediate')

    def critical(self, log_message):
        """
        This method adds log message on critical level.
        """ 

        self.log(log_message = log_message, log_level=4, write_mode ='immediate')


    def log(self, log_message, log_level, write_mode):
        """
        This method prepares the log and write the log to s3 
        """

        # only provide log output if log level of entry >= application log level
        if log_level >= self.app_log_level :
            #prepare log entry (python dictionary holding all values for a log entry)
            log_entry_dict = {
                'timestamp': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),
                'spark-app': self.log_conf.get('SPARK_APP_NAME'),
                'spark-app-id': self.log_conf.get('SPARK_APP_ID'),
                'job-object': self.log_conf.get('JOB_OBJECT'),
                'log-level': log_level, # log level of message (not app log level)
                'log-level-desc': self.log_level_desc[log_level], # descriptive of log level number
                'log-msg': log_message            
            }
            #print output to stdout if application log level is set to 0 = Debug
            if self.app_log_level == 0:
                print (str(log_entry_dict)) 
            ##add entry to log object
            self.append_line(log_entry_dict, write_mode)  

    def append_line (self, log_entry_dict, write_mode):
        """
        This method appends entry to log.
        """

        # convert python dict of log entry to JSON String
        json_line_str = json.dumps(log_entry_dict)
        if self.internal_log_mode == 'inmemory':
            self.write_inmemory_log(json_line_str)
        elif self.internal_log_mode == 'tmpfile': 
            self.write_tmp_log(json_line_str)   
        else:
            raise LoggerError("wrong internal log mode")
        ## write log to S3 (only when flush limit reached)   
        self.write_s3_log(write_mode)

    def write_tmp_log(self, line_str):
        """
        This method appends line in tmp log file.
        """

        #write log to local tmp folder and write to S3 decoupled/later
        tmp_log_file=open(self.tmp_log_file_path,'a')
        tmp_log_file.write(line_str+'\n')
        tmp_log_file.close() 
    
    def write_inmemory_log(self, line_str):
        """
        This method appends line to inmemory string (alternative to tmp log file).
        """

        #keep full LOG as string in memory and write to S3 directly 
        # create or append list of log entries
        try:
            if not self.inmemory_log: 
                self.inmemory_log = line_str+'\n'
            else:
                self.inmemory_log = self.inmemory_log + line_str+'\n'
        except:
            raise LoggerError("Unexpected error using 'inmemory' internal log mode")

    def write_s3_log(self, write_mode):
        """
        This method writes log to s3 (check if this should happen immediately or in a buffered mode)
        (write_mode 'buffered' for <flush_limit> lines of log or 'immediate')
        """

        self.flush_counter += 1    
        if write_mode == 'immediate' or self.flush_counter >= self.flush_limit :      
            # use log string if string has content or tmp file as data source for s3 push
            if self.inmemory_log :
                self.push_inmemory_log_to_s3() 
            else :  #push tmp log to s3
                self.push_tmp_log_to_s3() 
            self.flush_counter = 0
   
    def push_inmemory_log_to_s3(self):
        """
        This method pushes inmem log to s3. 
        """

        self.push_to_s3(self.inmemory_log)

    def push_tmp_log_to_s3(self):
        """
        This method pushes tmp log to s3. 
        """

        tmp_log_file = open(self.tmp_log_file_path,'rb')
        self.push_to_s3(tmp_log_file)
        tmp_log_file.close()

    def push_to_s3(self, data):
        """
        This method pushes log data to S3 object store (replace file on S3).
        """

        # boto3 putObject
        self.boto3client.put_object(Body=data, \
            Bucket = self.log_conf.get('LOG_S3_BUCKET'), \
            Key =  self.log_conf.get('LOG_S3_PREFIX')+ self.log_conf.get('LOG_FILE_NAME')
            )
    
    def __enter__(self):
        """
        This method marks the entry of the logging for any application.
        """

        self.info('Log of application started.')   
        self.debug(str(self.log_conf))     
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        This method marks the exit of the logging for any application.
        """
        if exc_value is not None:
            self.error(log_message = 'Exception: '+str(exc_type(exc_value)) )
            self.error(traceback.format_list(traceback.extract_tb(exc_traceback, limit=None)) )
        else :
            self.log(log_message = 'Log of application ended.', log_level=1, write_mode ='immediate')

class LoggerError(Exception):
    pass  