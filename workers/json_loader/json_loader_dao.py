__author__ = "Ivo Willemsen (IWIO)"
__date__ = "2018-11-26"

from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint, Numeric
from sqlalchemy.ext.declarative import declarative_base
from dbclients.DbClientFactory import DbClientFactory
from dbclients.DbClientConfig import DbClientConfig
from shared import get_logger, Singleton
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import ClauseElement
import arrow
from marshmallow_sqlalchemy import ModelSchema

Base = declarative_base()
SCHEMA = 'dashboard'
EXISTS = False
TIME = "time"
MACHINE = "machine"
ID = "id"
DESCRIPTION = "description"
MODEL_NAME = "model_name"
SIGNAL_NAME = "signal_name"
VALUE = "value"
NAME = "name"


class Limit(Base):
    """
    A limit is a horizontal line that is identified by an id and a machine.
    It has a description and a value
    """
    __table_args__ = tuple(UniqueConstraint("machine", "primary_id", name="lim_uc1"))
    __tablename__ = 'limits'

    id = Column(Integer, primary_key=True, autoincrement=True)
    primary_id = Column(String(100), index=True)
    machine = Column(String(10), index=True)
    description = Column(String(100), index=True)

    value = Column(Numeric(precision=15, scale=5, asdecimal=True))

    def __repr__(self): return "<Limit(id=%s, primary_id=%s, machine=%s, " \
                               "description=%s, >, value=%s", \
                               self.id, self.primary_id, self.machine, \
                               self.description, self.value

    @staticmethod
    def update(current, new):
        """
        This function updates the current limit with the new values
        :param current: the current row
        :param new: the new row
        """
        current.value = new[VALUE]
        current.description = new[DESCRIPTION]


class LimitSchema(ModelSchema):
    class Meta:
        model = Limit


limit_schema = LimitSchema()


class Annotation(Base):
    __tablename__ = 'annotations'
    __table_args__ = tuple(UniqueConstraint("machine", "model_name", "primary_id", name="ann_uc1"))

    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(DateTime)
    machine = Column(String(10), index=True)
    model_name = Column(String(100), index=True)
    primary_id = Column(String(100), index=True)
    description = Column(String(100), index=True)

    def __repr__(self):
        return "<Annnotation(id=%s, time=%s, machine=%s, model_lanem=%s, primary_id=%s, description=%s )>" % \
               (self.id, self.time, self.machine, self.model_name, self.primary_id, self.description)

    @staticmethod
    def update(current, new):
        current.TIME = new[TIME]
        current.DESCRIPTION = new[DESCRIPTION]


class AnnotationSchema(ModelSchema):
    class Meta:
        model = Annotation


annotation_schema = AnnotationSchema()


@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    session = sessionmaker(bind=engine, autoflush=False)()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class JsonDao:
    """
    Class that represents the DAO that takes care of handling communication
    with the underlying relational database schema
    """
    __metaclass__ = Singleton

    def __init__(self, logger=None):
        """
        Initialization of GenericJsonDao
        :param logger: logger
        """
        self._logger = logger or get_logger('TEST')
        try:
            self._dbclient = DbClientFactory.get_client(DbClientConfig.get(SCHEMA), self._logger)
            self._engine = self.dbclient.get_engine()
            Base.metadata.create_all(self._engine)
        except Exception as e:
            self._logger.exception("Unable to initialize MySQL " + SCHEMA + " scheme: " + str(e))

    def save_or_update(self, model, values=None, **kwargs):
        """
        Returns instance of the specified item:
         - if existing retrieved from the database
         - if non-existing created on the database
        :param session:   the session object
        :param model:     object type (or table)
        :param values:    non-unique values used
        :param kwargs:    attributes that uniquely identify item
        :return: -
        """

        with session_scope(self.dbclient.get_engine()) as session:
            instance = session.query(model).filter_by(**kwargs).first()
            if instance:
                model.update(instance, values)
            else:
                params = dict((k, v) for k, v in kwargs.iteritems() if not isinstance(v, ClauseElement))
                params.update(values or {})
                instance = model(**params)
                session.add(instance)

    def add_annotation(self, annotation, machine, model_name):
        """
        Adds an annotation to the database.
        :param annotation: The annotation to be added
        :param machine: The machine
        :param model_name: The name of the model that needs to be specified
        :return: -
        """

        self.save_or_update(
            model=Annotation,
            machine=machine, model_name=model_name, primary_id=annotation[ID],                   # ids
            values={DESCRIPTION: annotation[DESCRIPTION], TIME: self.to_utc(annotation[TIME])})  # values

    def add_limit(self, limit, machine):
        """
        Adds a limit to the database.
        :param limit: The limit to be added
        :param machine: The machine
        :return: -
        """
        self.save_or_update(
            model=Limit,
            machine=machine, primary_id=limit[ID],                          # ids
            values={DESCRIPTION: limit[DESCRIPTION], VALUE: limit[VALUE]})  # values

    def get_limit(self, _id):
        """
        This dao function return the limit that is identified by the passed id
        :param _id: the id of the limit
        :return: the limit row
        """
        with session_scope(self.dbclient.get_engine()) as session:
            return limit_schema.dump(session.query(Limit).filter_by(primary_id=_id).first()).data

    def get_all_limits(self):
        """
        This dao functions returns all the limits that can be found in the
        database table limits
        """
        with session_scope(self.dbclient.get_engine()) as session:
            return map(lambda x: limit_schema.dump(x).data, session.query(Limit).all())

    def get_all_annotations(self, model_name):
        """
        This dao functions returns all the annotations that can be found in the
        database table annotations that
        :param: model_name: the name of the model of which annotations should be returned
        """
        with session_scope(self.dbclient.get_engine()) as session:
            return map(lambda x: annotation_schema.dump(x).data,
                       session.query(Annotation).filter_by(model_name=model_name).all())

    @property
    def dbclient(self):
        return self._dbclient

    @property
    def engine(self):
        return self._engine

    @staticmethod
    def to_utc(timestamp):
        return arrow.get(timestamp).to("UTC").naive
