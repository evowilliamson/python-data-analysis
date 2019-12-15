import json
import os
from misc.singleton import Singleton
import shared as sh
from tools.ondemand import attribute
from tools.ondemand.attribute import Attribute


TASK_LOGGER = sh.get_logger(sh.TASK_LOG)
LOCATION = "location"
ROOT = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = "../../config/ondemand"
ONDEMAND_JSON = "ondemand.json"
DIR_PATTERN = "dir_pattern"

DBUSER = "dbuser"
DBPASSWORD = "dbpassword"
DBHOST = "dbhost"
DBNAME = "dbname"
DBPORT = "dbport"

UPDATED = "Updated"
NEW = "New"
DELETED = "Deleted"

ATTRIBUTES = {attribute.ID: Attribute(attribute.ID, attribute.ID, attribute.REQUEST + "_" + attribute.ID),
              attribute.DESCRIPTION: Attribute(attribute.DESCRIPTION, attribute.DESCRIPTION, attribute.DESCRIPTION),
              attribute.STARTTIME: Attribute(attribute.STARTTIME, attribute.STARTTIME, "start_time"),
              attribute.ENDTIME: Attribute(attribute.ENDTIME, attribute.ENDTIME, "end_time"),
              attribute.OWNER: Attribute(attribute.OWNER, attribute.OWNER, attribute.OWNER),
              attribute.EMAIL: Attribute(attribute.EMAIL, attribute.EMAIL, attribute.EMAIL + "s")}

EXPIRE_PERIOD = "expire_period"


@Singleton
class Config:
    """ Class that represents the configurations in this application."""

    def __init__(self):
        """ Default constructor """
        self._config = {}

    def load(self):
        # mounts = sh.Mounts()
        with open(os.path.join(ROOT, CONFIG_DIR, ONDEMAND_JSON)) as f:
            self._config = json.load(f)

    @property
    def config(self):
        return self._config
