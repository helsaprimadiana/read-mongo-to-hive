from src import *

spark = get_spark()

if JDBC_TYPE.upper() == "DB2LUW":
    JDBC_URL = "jdbc:db2://{0}:{1}/{2}".format(JDBC_HOST, JDBC_PORT, JDBC_SCHEMA)
    JDBC_CLASS = "com.ibm.db2.jcc.DB2Driver"

elif JDBC_TYPE.upper() == "AS400":
    JDBC_URL = "jdbc:as400://{0}".format(JDBC_HOST)
    JDBC_CLASS = "com.ibm.as400.access.AS400JDBCDriver"

elif JDBC_TYPE.upper() == "MYSQL":
    JDBC_URL = "jdbc:mysql://{0}:{1}/{2}".format(JDBC_HOST, JDBC_PORT, JDBC_SCHEMA)
    JDBC_CLASS = "com.mysql.jdbc.Driver"

elif JDBC_TYPE.upper() == "SQLSERVER":
    JDBC_URL = "jdbc:sqlserver://{0}:{1};databaseName={2};schema={3}".format(JDBC_HOST, JDBC_PORT, JDBC_DATABASE, JDBC_SCHEMA)
    JDBC_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

elif JDBC_TYPE.upper() == "SQLSERVER_INSTANCE":
    JDBC_URL = "jdbc:jtds:sqlserver://{0}:{1}/{2};instance={3}".format(JDBC_HOST, JDBC_PORT, JDBC_SCHEMA,
                                                                       JDBC_INSTANCE)
    JDBC_CLASS = "net.sourceforge.jtds.jdbc.Driver"

elif JDBC_TYPE.upper() == "ORACLE":
    JDBC_URL = 'jdbc:oracle:thin:{0}/{1}@{2}:{3}/{4}'.format(JDBC_USER, JDBC_PASSWORD, JDBC_HOST, JDBC_PORT, JDBC_SID)
    JDBC_CLASS = "oracle.jdbc.OracleDriver"

elif JDBC_TYPE.upper() == "HIVE":
    JDBC_URL = "jdbc:hive2://{0}:{1}/;serviceDiscoveryMode=zooKeeper;".format(JDBC_HOST,JDBC_PORT)
    JDBC_CLASS = "org.apache.hive.jdbc.HiveDriver"

def read_jdbc_query(query):
    logger.info("{0}".format(query))
    df = spark.read \
        .format("jdbc") \
        .option("driver", JDBC_CLASS) \
        .option("url", JDBC_URL) \
        .option("dbtable", "({0})x".format(query)) \
        .option("user", JDBC_USER) \
        .option("password", JDBC_PASSWORD) \
        .load()
    return df


def read_jdbc_table(table_name):
    df = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", JDBC_USER) \
        .option("password", JDBC_PASSWORD) \
        .load()
    return df

def write_jdbc_table(df, table_name):
    df.write.mode(JDBC_WRITE_MODE) \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "{0}.{1}".format(JDBC_SCHEMA, table_name)) \
        .option("user", JDBC_USER) \
        .option("password", JDBC_PASSWORD) \
        .option("driver", JDBC_CLASS) \
        .save()
    logger.info("========= INSERT INTO TABLE {0} DONE============".format(table_name))
