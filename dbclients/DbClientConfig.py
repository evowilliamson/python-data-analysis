from misc.singleton_metaclass import Singleton
import os
import json
import base64

ROOT = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = "../config"
DBCONFIG_JSON = "dbconfig.json"


HOST = "host"
PORT = "port"
DATABSE = "database"
USER = "user"
PASSWORD = "password"
DB_TYPE = "db_type"


class DbClientConfig:
    """
    Class that represents a database client configuration
    """
    __metaclass__ = Singleton

    def __init__(self, db_type, host, port, database, user, password):
        """
        Constructor to instantiate a database client configuration
        :param db_type: the database type, a string that maps to
               DbClientFactory.DbType.name
        :param host: the host on which the database runs
        :param port: the port
        :param database: the name of the database
        :param user: the user which is used to connect
        :param password: the password of the configured user
        """
        self._db_type = db_type
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password

    @classmethod
    def get(cls, db_config_id):
        """
        Gets a database client configuration instance
        :param db_config_id: the id of the database configuration
        :return: an instance of DbClientConfig
        """
        with open(os.path.join(ROOT, CONFIG_DIR, DBCONFIG_JSON)) as f:
            db_config = json.load(f)

        return DbClientConfig(
            db_type=db_config[db_config_id][DB_TYPE], host=db_config[db_config_id][HOST],
            port=db_config[db_config_id][PORT], database=db_config[db_config_id][DATABSE],
            user=db_config[db_config_id][USER],
            password=base64.b64decode(db_config[db_config_id][PASSWORD]))

    @property
    def db_type(self):
        return self._db_type

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def database(self):
        return self._database

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password
