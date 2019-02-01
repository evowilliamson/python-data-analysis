from misc.singleton_metaclass import Singleton
from PostgresClient import PostgresClient
from MySQLClient import MySQLClient
from enum import Enum, unique


@unique
class DbType(Enum):
    """
    Enum that represents all sorts of database type that could be used
    """
    INFLUX = 1
    MYSQL = 2
    POSTGRES = 3


class DbClientFactory:
    """
    the database client factory that creates instances of database clients
    """
    __metaclass__ = Singleton

    db_clients = {DbType.POSTGRES: PostgresClient,
                  DbType.MYSQL: MySQLClient}

    def __init__(self):
        pass

    @classmethod
    def get_client(cls, db_client_config, logger):
        """
        Function that creates a database client based on a preferred configuration.
        The configuration is done in config/dbclient.json
        :param db_client_config: the object that contains the db client configuration
        :param logger: the logger that is passed and used in the database client
        log errors in case things go wrong during database actions
        :return: the database client instance
        """
        db_type = next(db_type for db_type in DbType if db_type.name == db_client_config.db_type)
        return DbClientFactory.db_clients[db_type](db_client_config, logger)


