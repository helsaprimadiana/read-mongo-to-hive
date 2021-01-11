from src import *
from src.utils.jdbc import write_jdbc_table

_spark = get_spark()

def storage2jdbc(sql_script, jdbc_table, buss_date):
    logger.info("Load ods from {0} to {1} started.".format(sql_script, jdbc_table))
    sql_path = SQL_INGEST_PATH

    with open(sql_path + sql_script) as fr:
        file_content = fr.read()
        query = file_content.format(ODS_SCHEMA)
        # TODO: change default schema to parameterized

        if buss_date != '0':
            query += " AND BUSS_DATE = '{}' ".format(buss_date)

        logger.info(query)
        # TODO: need to recreate

        df_sql = _spark.sql(query)
        write_jdbc_table(df_sql, jdbc_table)
        logger.info("Write data to {0} finished.".format(jdbc_table))
