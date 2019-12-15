__author__ = "Ivo Willemsen (IWIO)"
__date__ = "2018-11-26"

from shared import load_file
import json
import pandas as pd
from shared import Singleton, get_logger
from workers.json_loader.json_loader_dao import JsonDao

TIMESERIES = "time_series"
TIMEFORMAT = "time_format"
UTC = "UTC"
DATA = "data"
VALUE = "value"
INDEX = "time"
NAME = "name"
ANNOTATIONS = "annotations"
LIMITS = "limits"
MACHINE = "machine_number"
MODEL_NAME = "model"


class JsonLoaderService:
    """
    Class that represents a generic json file that is used to import data into a time series and
    relational database. The `get_time_series_data` function will extract the data contained in the
    timeseries.data fields and yields it to be insetted in the time series db influxdb.

    Relational data is inserted in the Mysql db
    """
    __metaclass__ = Singleton

    def __init__(self, logger=None):
        """
        Constructor to initialize the an instance of type JsonLoaderService
        :param logger: the logger to be used
        """
        self._logger = logger or get_logger('TEST')
        self.dao = JsonDao(logger)

    @staticmethod
    def load_data(file_name):
        """
        This function loads the file and converts it into json format
        :param file_name: the name of the file
        :return: the converted file
        """
        return json.load(load_file(file_name))

    @staticmethod
    def get_time_series_data(data):
        """
        This method retrieves the series of data points
        :param data: the json field that ocntains the time series data
        :return: a dataframe with the time series data
        """
        if not data.get(TIMESERIES):
            return
        return [JsonLoaderService.__create_time_series_df(signal) for signal in data[TIMESERIES]]

    def write_annotations(self, data):
        """
        Writes the annotations that have been found in the json to the relational database
        :param data: the json field that contains the annotations
        :return:-
        """

        if not data.get(ANNOTATIONS):
            return
        for annotation in data[ANNOTATIONS]:
            self.dao.add_annotation(annotation, data[MACHINE], data[MODEL_NAME])

    def write_limits(self, data):
        """
        Writes the limits that are found in the json to the relational database
        :param data: the json field that contains the limits
        :return:-
        """
        if not data.get(LIMITS):
            return
        for limit in data[LIMITS]:
            self.dao.add_limit(limit, data[MACHINE])

    @staticmethod
    def __create_time_series_df(signal):
        """

        :param signal: the signal
        :return: the created dataframe
        """

        lot = [(pd.Timestamp(datapoint[INDEX]).tz_convert(UTC), datapoint[VALUE]) for datapoint in signal[DATA]]
        return pd.DataFrame(data={signal[NAME]: list(zip(*lot)[1])}, index=list(zip(*lot)[0]))

    def get_limit(self, _id):
        """
        Service call to a dao in order to return the limit that conform to
        a certain id
        :param _id: the id that is used to retrieve the limit
        :return: the limit
        """
        return self.dao.get_limit(_id)

    def get_all_limits(self):
        """
        Service call to a dao in order to return all the limits
        :return: a list of limits
        """
        return self.dao.get_all_limits()

    def get_all_annotations(self, model_name):
        """
        Service call to a dao in order to return all the annotations
        :param model_name: the name of the model of which all annotations should be retrieved
        :return: a list of annotations
        """
        return self.dao.get_all_annotations(model_name)
