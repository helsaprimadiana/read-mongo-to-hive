from src.utils.spark_factory import get_spark
from src.utils.logger import Logger
from yaml import full_load
from os import path
import sys

logger = Logger()

APPLICATION_PATH = path.abspath(sys.modules['__main__'].__file__)[:-6]
CONFIGURATION_PATH = APPLICATION_PATH
config_file = CONFIGURATION_PATH + 'config.yaml'

logger.info(config_file)

with open(config_file) as file:
    config = full_load(file)

    SQL_STORAGE_PATH = CONFIGURATION_PATH+"sql_storage/"
    SQL_INGEST_PATH = CONFIGURATION_PATH +"sql_ingest/"
    HIST_SCHEMA = config['hist_schema']

    STAGING_SCHEMA = config['staging_schema']

    JDBC_TYPE = config['jdbc_type']
    JDBC_HOST = config['jdbc_host']
    JDBC_PORT = config['jdbc_port']
    JDBC_DATABASE = config['jdbc_database']
    JDBC_SCHEMA = config['jdbc_schema']
    JDBC_INSTANCE = config['jdbc_instance']
    JDBC_SID = config['jdbc_sid']
    JDBC_USER = config['jdbc_user']
    JDBC_PASSWORD = config['jdbc_password']
    JDBC_WRITE_MODE = config['jdbc_write_mode']
    MONGO_USERNAME = config['mongo_username']
    MONGO_PASSWORD = config['mongo_password']
    MONGO_URL = config['mongo_url']
    MONGO_REPLICASET = config['mongo_replicaset']
    SCHEMA_MONGO_PATH = CONFIGURATION_PATH+config['schema_table_mongo_path']





