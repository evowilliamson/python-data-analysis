ID = "id"
DESCRIPTION = "description"
STARTTIME = "starttime"
ENDTIME = "endtime"
OWNER = "owner"
EMAIL = "email"
REQUEST = "request"


class Attribute:
    """
    Class that represents the concept of Attribute
    """

    def __init__(self, concept, input_source, database):
        """
        Default constructor that creates an attribute
        :param concept:
        """
        self._concept = concept
        self._input_source = input_source
        self._database = database

    @property
    def concept(self):
        return self._concept

    @property
    def input_source(self):
        return self._input_source

    @property
    def database(self):
        return self._database
