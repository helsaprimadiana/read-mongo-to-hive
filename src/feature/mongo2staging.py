from src.utils.spark_factory import get_spark
from src import *
import urllib.parse
from pyspark.sql.functions import struct,to_json,lit


_spark=get_spark()
sc=_spark.sparkContext

usr = urllib.parse.quote_plus(MONGO_USERNAME)
pss = urllib.parse.quote_plus(MONGO_PASSWORD)

def mongo2staging(db_cl_name,buss_date):
    dictionary_search = db_cl_name.lower().replace(".","_").replace("-","_")
    logger.info("Read Data {0} from MongoDB : START".format(db_cl_name))
    file_schema = open(SCHEMA_MONGO_PATH + dictionary_search+".json", "r", encoding="utf-8")

    template_schema = file_schema.read()
    schema_base = _spark.read.json(sc.parallelize([template_schema])).schema
    mg_spark = _spark.builder \
        .appName("mongodb_to_hive") \
        .config("spark.mongodb.input.uri","mongodb://{0}:{1}@{2}/{3}?authSource=admin&replicaSet={4}".format(usr,pss,MONGO_URL,db_cl_name,MONGO_REPLICASET)) \
        .getOrCreate()

    df_mongo = mg_spark.read.format("mongo").schema(schema_base).load()
    logger.info("Read Data {0} from MongoDB : END".format(db_cl_name))

    logger.info("Transforming Data {0} in Datalake Staging : START".format(db_cl_name))

    df_load=df_mongo.withColumn('json_temp',struct(df_mongo.columns))\
        .withColumn('buss_date',lit(buss_date))
    df_json=df_load.select('_id', to_json(df_load.json_temp).alias('json'),'buss_date')
    df_json.createOrReplaceTempView("df_mongo_temp")

    logger.info("Insert into {0}.{1} in Datalake Staging : START".format(STAGING_SCHEMA, dictionary_search))
    _spark.sql("""INSERT OVERWRITE TABLE  {0}.{1}
         select * from df_mongo_temp t """.format(STAGING_SCHEMA, dictionary_search))
    logger.info("Insert into {0}.{1} in Datalake Staging : END".format(STAGING_SCHEMA, dictionary_search))

    logger.info("Transforming Data {0} in Datalake Staging : END".format(db_cl_name))
