""" This module is responsible for setting connection with postgress and loading dataframe in Database Table."""
from sqlalchemy import create_engine, text
import pandas as pd
import json
from logs.etl_log import logger

class Connection:
    def __init__(self,config_file):
        """This Constructror takes config file path while creating object of this class"""
        self.config_file = config_file

        # Creating object of file which provided as input parameter and reading file in r mode
        with open(self.config_file, 'r') as config_object:
            self.db_config = json.load(config_object)
        # Accessing all the configuration values from the config.json file 
        self.username = self.db_config["database"]["username"]
        self.password = self.db_config["database"]["password"]
        self.host = self.db_config["database"]["host"]
        self.port = self.db_config["database"]["port"]
        self.database_name = self.db_config["database"]["database_name"]

        # Creating Database Connection string
        self.connection_string = f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"
    
    def established_connection(self):
        connection_object = create_engine(self.connection_string)
        try:
            postgres_connection = connection_object.connect()
            if postgres_connection:
                print('Connection Successfull')
                return connection_object
        except Exception as e:
            logger.error('Connection Filed due Database issue',e)

class DataFrameLoading:
    """ This class is responsible for executing SQL query"""
    def __init__(self, connection_object, dataframe, table_name):
        self.connection_object = connection_object
        self.dataframe = dataframe
        self.table_name = table_name
    
    def data_load(self):
        try:
            self.dataframe.to_sql(self.table_name, self.connection_object, if_exists="replace", index=False)
            return True
        except Exception as e:
            logger.error(f'There is an Error while loadin the data into {self.table_name}')




class DataLoading:
    def __init__(self,result_df):
        self.result_df = result_df
    
    def loading(self):
        # Creating Database connection object 
        con = Connection('/home/sheikjb/bollydata/credentials.json')
        # Testing Connection
        connection_object = con.established_connection()

        dataframe_loading = DataFrameLoading(connection_object, self.result_df, "movies_details")
        if dataframe_loading.data_load():
            logger.info('Data Loading completed successfully for table movies_details')