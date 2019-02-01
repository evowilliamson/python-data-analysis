import json
import os
from misc.singleton import Singleton

ROOT = os.path.dirname(os.path.realpath(__file__))
CONFIG_INPUT_PATH = "S:\\ID"
CONFIG_OUTPUT_PATH = "T:\\etl\\SDDA_Data"
CONFIG_DIR = "..\\..\\config"
TREECOPY_MACHINES_JSON = "copyfiles_machines.json"
TREECOPY_SOURCES_HANDLERS_JSON = "copyfiles_sources_handlers.json"


@Singleton
class Config:
    """ Class that represents the configurations in this application."""

    def __init__(self):
        """ Default constructor """

        with open(os.path.join(ROOT, CONFIG_DIR, TREECOPY_MACHINES_JSON)) as f:
            self._machines = json.load(f)

        with open(os.path.join(ROOT, CONFIG_DIR, TREECOPY_SOURCES_HANDLERS_JSON)) as f:
            self._sources_handlers = json.load(f)

    @property
    def machines(self):
        """ @property-decorated method that retrieves the machines in the configuration"""
        return self._machines

    @property
    def sources_handlers(self):
        """ @property-decorated method that retrieves the sources handlers in the configuration"""
        return self._sources_handlers
