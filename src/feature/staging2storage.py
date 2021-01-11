from src.utils.spark_factory import get_spark
from src import *
import urllib.parse

_spark=get_spark()

def staging2storage(landing_script, hist_table, buss_date):

    logger.info("Load Landing from {0} to ods.{1} started.".format(landing_script, hist_table))
    sql_path = SQL_STORAGE_PATH

    with open(sql_path + landing_script) as fr:
        file_content = fr.read()

        query = file_content % (STAGING_SCHEMA)
        if buss_date != '0':
            query += " AND BUSS_DATE = '{}' ".format(buss_date)

        logger.error(query)

        df_sql = _spark.sql(query)
        df_sql.createOrReplaceTempView('{0}'.format(hist_table))


        logger.info("Writing to Storage...")
        table_list_hist = _spark.sql("show tables in {0}".format(HIST_SCHEMA))
        table_name_hist = table_list_hist.filter(table_list_hist.tableName == "{0}".format(hist_table)) \
            .filter(table_list_hist.isTemporary == "false").collect()
        if len(table_name_hist) > 0:
            _spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
            _spark.sql('''
                          INSERT OVERWRITE TABLE {0}.{1} partition(buss_date)
                          SELECT * FROM {1}
                      '''.format(HIST_SCHEMA, hist_table))
        else:
            logger.error("Table Not Existed")


    logger.info("Write data to {0}.{1} finished.".format(HIST_SCHEMA, hist_table))




