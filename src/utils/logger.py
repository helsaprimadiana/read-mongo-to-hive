from src.utils.spark_factory import get_spark


class Logger:
    def __init__(self):
        self.sc = get_spark().sparkContext
        self.logger = self._logger_factory(self, self.sc)

    @staticmethod
    def _logger_factory(self, sc):
        log4jLogger = sc._jvm.org.apache.log4j
        logger = log4jLogger.LogManager.getLogger(__name__)
        return logger

    def info(self, msg):
        """
        Writes INFO-level message
        :param msg: message to log
        :return:
        """
        self.logger.info('--------------------------------------------------------------------------------------------')
        self.logger.info(msg)
        self.logger.info('--------------------------------------------------------------------------------------------')

    def warning(self, msg):
        """
        Writes WARNING-level message
        :param msg: message to log
        :return:
        """
        self.logger.warning('-----------------------------------------------------------------------------------------')
        self.logger.warning(msg)
        self.logger.warning('-----------------------------------------------------------------------------------------')

    def error(self, msg):
        """
        Writes ERROR-level message
        :param msg: message to log
        :return:
        """
        self.logger.error('-------------------------------------------------------------------------------------------')
        self.logger.error(msg)
        self.logger.error('-------------------------------------------------------------------------------------------')

    def infodetails(self, msg):
        """
        Writes INFO-level message details
        :param msg: message to log
        :return:
        """
        self.logger.info('>>>>> '+msg)
