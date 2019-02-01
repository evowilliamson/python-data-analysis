import psycopg2
from datetime import datetime


class PostgresClient:
    """
    Class that represents a Postgres client
    """

    def __init__(self, db_client_config, logger):
        """
        Constructor for creating an instance of the PostgresClient
        :param db_client_config: the configuration for the client to use
        :param logger: an instance of logger
        """
        self.connect_string = "dbname={0} user={1} password={2} host={3} port={4}".format(
                db_client_config.database,
                db_client_config.user,
                db_client_config.password,
                db_client_config.host,
                db_client_config.port)
        self.connection = None
        self.open_connection()
        self.cursor = None
        self.logger = logger

    def get_all(self, query_string, params, retry_count=3):
        """
        Get all rows that conform to the conditions. It contains a retry
        mechanism that allows for fault recovery
        :param query_string: the string that contains the SQL
        :param params: parameters that should be replaced in the query_string
        :param retry_count: the number of times query should be retried in case
               of error
        :return: the retrieved rows from the database as the first element in the tuple,
        followed by the number of errors (retry_count) and then the elapsed time in minutes
        """
        last_exception = None
        for i in range(retry_count):
            try:
                self.open_cursor()
                t1 = datetime.utcnow()
                self.cursor.execute(query_string, tuple(params))
                results = self.cursor.fetchall()
                return results, i, (datetime.utcnow() - t1).seconds/60.0
            except Exception as e:
                last_exception = e
                self.log(query_string, params, e, i)
                self.logger.warning("Closing and opening again new connection to get around resource problems")
                self.close_connection()
                self.open_connection()
            finally:
                self.close_cursor()

        raise last_exception

    def close_connection(self):
        self.connection.close()

    def open_connection(self):
        self.connection = psycopg2.connect(self.connect_string)

    def open_cursor(self):
        self.cursor = self.connection.cursor()

    def close_cursor(self):
        self.cursor.close()

    def log(self, query_string, params, exception, i):
        """
        Logs the error in case something goes wrong
        :param query_string: the query string
        :param params: the parameters
        :param exception: the exception
        :param i: index of i in the total of retries
        :return: -
        """
        if self.logger:
            self.logger.error(
                "Retry: {0}, Error when getting rows. "
                "Error: {1}, query: {2}, params: {3}".format(
                    str(i + 1), str(exception), query_string,
                    ','.join(str(p) for p in params)))



