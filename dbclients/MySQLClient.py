from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import ClauseElement

from shared import get_logger


class MySQLClient:
    """
    Class that represents a MySQL client
    """

    def __init__(self, db_client_config, logger=None):
        """
        Constructor for creating an instance of the MySQLClient
        :param db_client_config: the configuration for the client to use
        :param logger: an instance of logger
        """
        self._connect_string = "mysql://{0}:{1}@{2}/{3}".format(
            db_client_config.user,
            db_client_config.password,
            db_client_config.host,
            db_client_config.database)
        self._logger = logger or get_logger('TEST')

        self._engine = None
        self._session = None

    def get_engine(self):
        """
        Returns MySQL engine instance
        :return: engine
        """
        if self._engine is None:
            self._engine = create_engine(self._connect_string, echo=False)
        return self._engine

    def get_session(self):
        """
        Returns MySQL session instance
        :return: session
        """
        if self._session is None:
            session = sessionmaker(bind=self._engine, autoflush=False)
            self._session = session()
        return self._session

    def get_or_create(self, model, defaults=None, **kwargs):
        """
        Returns instance of the specified item:
         - if existing retrieved from the database
         - if non-existing created on the database
        :param model:     object type (or table)
        :param defaults:  default values used for creation
        :param kwargs:    attributes that uniquely identify item
        :return:          instance of item, plus:
                          - False if retrieved
                          - True if created
        """
        session = self.get_session()
        instance = session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance, False
        else:
            params = dict((k, v) for k, v in kwargs.iteritems() if not isinstance(v, ClauseElement))
            params.update(defaults or {})
            instance = model(**params)
            session.add(instance)
            return instance, True
