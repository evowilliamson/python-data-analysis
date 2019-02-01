import utils
from config import Config
from config import ATTRIBUTES, LOCATION
import attribute
from bs4 import BeautifulSoup
from request import Request
from misc.singleton import Singleton
from attribute import REQUEST

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


@Singleton
class InputSource:
    """
    Class responsable for handling the input. It reads the input file and generates a list of request
    objects
    """

    def __init__(self):
        """ Default constructor """

        self.requests = \
            [Request(utils.get_text(request.find(ATTRIBUTES[attribute.ID].input_source)),
                     self.create_component(request),
                     utils.get_text(request.find(ATTRIBUTES[attribute.DESCRIPTION].input_source)),
                     utils.get_timestamp(request.find(ATTRIBUTES[attribute.STARTTIME].input_source), TIMESTAMP_FORMAT),
                     utils.get_timestamp(request.find(ATTRIBUTES[attribute.ENDTIME].input_source), TIMESTAMP_FORMAT),
                     utils.get_text(request.find(ATTRIBUTES[attribute.OWNER].input_source)),
                     ",".join([utils.get_text(email)
                               for email in request.findAll(ATTRIBUTES[attribute.EMAIL].input_source)]))
             for request in BeautifulSoup(open(Config.instance().config[LOCATION]), "lxml")
                .findAll(REQUEST)]
        print("Number of requests found in input file: {0}".format(len(self.requests)))

    def requests(self):
        return self.requests

    @classmethod
    def create_component(cls, request):
        """
        This method creates the component. A component constist of the tag 'component'
        concatenated with the gos key, which is not mandatory
        :param request: the request html tag
        :return: The contacted component
        """

        component = utils.get_text(request.find("component"))
        gos_key = utils.get_text(request.find("goskey"))
        if gos_key:
            component += "." + gos_key
        return component
