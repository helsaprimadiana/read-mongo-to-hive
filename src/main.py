from src import *
from src.feature.mongo2staging import mongo2staging
from src.feature.staging2storage import staging2storage
from src.feature.storage2jdbc import storage2jdbc

MODULE = sys.argv[1]
SQL = sys.argv[2]
TABLE = sys.argv[3]
BUSS_DATE = sys.argv[4]

def run():
    """
    Main method
    :return:
    """
    logger = Logger()
    logger.info("Module {} Start".format(MODULE))

    if MODULE.lower() == "mongo2staging":
        mongo2staging(TABLE,BUSS_DATE)

    elif MODULE.lower() == "staging2storage":
        staging2storage(SQL, TABLE, BUSS_DATE)

    elif MODULE.lower() == "storage2jdbc":
        storage2jdbc(SQL, TABLE, BUSS_DATE)
