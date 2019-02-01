from misc.singleton import Singleton
from sqlalchemy import create_engine
from tools.ondemand.config import Config, DBUSER, DBPASSWORD, DBHOST, DBPORT, DBNAME, ATTRIBUTES, DELETED, UPDATED, NEW
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc
from tools.ondemand.dao.request import Request
import shared as sh
import traceback
import datetime

DB_URI = "mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
TASK_LOGGER = sh.get_logger(sh.TASK_LOG)


@Singleton
class Dao:
    """ This class contains the logic that takes care of interaction with the database """

    def __init__(self):
        """ Constructor that initializes the DB engine and creates the session """
        config = Config.instance()
        self._engine = create_engine(DB_URI.format(
            user=config.config[DBUSER],
            password=config.config[DBPASSWORD],
            host=config.config[DBHOST],
            port=config.config[DBPORT],
            db=config.config[DBNAME]
        ))
        self.Session = sessionmaker(bind=self._engine)
        self._session = self.Session()

    def synch(self, requests):
        """
        This method synchronizes the input requests with the information that
        is present in the database
        :param requests: the list of requests
        :return:
        """

        try:
            # First get all the requests from the database
            persisted_requests = self.get_all_requests()
            # Process requests that should be saved or updated
            self.save_or_update(requests, persisted_requests)
            # Requests that are not reported anymore should be logically removed from the db
            self.delete(requests, persisted_requests)
        except exc.SQLAlchemyError as e:
            # TASK_LOGGER.error("Error when communicating with database: " + traceback.format_exc(e))
            print("Error when communicating with database: " + traceback.format_exc(e))
        finally:
            self._session.close()

    def get_all_requests(self):
        """
        Method that retrieves all requests from the db
        :return:
        """
        return self._session.query(Request).all()

    def save_or_update(self, requests, persisted_requests):
        """
        Requests that exist in the input file and don't exist in the db should be added to the db. Requests that
        already exists, should be checked if they have changed. If so, sync with the input
        :param requests: the list of requests that come from the input
        :param persisted_requests: list of the requests that come from the db
        :return:
        """
        for request in requests:
            validated = False
            if request.check():
                validated = True
            persisted_request = self.search_persisted(request, persisted_requests)
            if not persisted_request:
                # Add the request to the database
                request_orm = Request(id=request.id, description=request.description, component=request.component,
                                      start_time=request.start_time, end_time=request.end_time,
                                      emails=request.emails, owner=request.owner, validated=validated,
                                      last_updated=datetime.datetime.now(), update_action=NEW)
                print("New: " + str(request))
                self._session.add(request_orm)
            else:
                if not self.equals(persisted_request, request) or persisted_request.update_action == DELETED:
                    update_action = UPDATED
                    if persisted_request.update_action == DELETED:
                        print("Deleted before, but added again: " + str(request))
                        update_action = DELETED + "-" + NEW
                    persisted_request.update(request, validated, update_action)
                    print("Changed: " + str(request))
        self._session.commit()

    def delete(self, requests, persisted_requests):
        """
        Method that logically deletes requests from the db by setting a flag
        :param requests: the list of requests that come from the input
        :param persisted_requests: a list of persisted (db) requests
        :return:
        """
        for persisted_request in persisted_requests:
            if not self.search_input(persisted_request, requests) and not persisted_request.update_action == DELETED:
                print("Deleted: " + str(persisted_request))
                persisted_request.update(persisted_request, True, DELETED)
        self._session.commit()

    @classmethod
    def create_column_list(cls):
        """
        This method creates a list of columns seperated by commas to be used in the select clause
        :return: The list of columns seperated by commas
        """

        result = ""
        for key, attribute in ATTRIBUTES.items():
            result += attribute.database + ","
        return result[:-1]

    @classmethod
    def search_persisted(cls, request, persisted_requests):
        """
        Check if the request already exist in the list of persisted requests
        :param request: the ongoing request
        :param persisted_requests:  the list of persisted requests
        :return: the persisted row, None if not a single on generated
        """

        for persisted_request in persisted_requests:
            if persisted_request.id == request.id:
                return persisted_request
        return None

    @classmethod
    def search_input(cls, persisted_request, requests):
        """
        Check if the persisted request is still in the input. If it is not, return False
        :param persisted_request: the persisted request being investigated
        :param requests: the list of incoming requests
        :return: False if the persisted request can not be found in the list of incoming requests
        """

        for request in requests:
            if persisted_request.id == request.id:
                return True
        return False

    @staticmethod
    def equals(persisted, other):
        """
        Static method that checks whether the two passed request objects are the same
        :param persisted: the persisted object (db)
        :param other: the input source request object
        :return: True if the same, otherwise False
        """

        if persisted.id == other.id and persisted.component == other.component and \
            persisted.description == other.description and persisted.start_time == other.start_time and \
            persisted.end_time == other.end_time and persisted.owner == other.owner and \
                persisted.emails == other.emails:
            return True
        else:
            return False




