import shared as sh
import re

TASK_LOGGER = sh.get_logger(sh.TASK_LOG)


class Request:
    """
    This class represents a request for data access
    """

    def __init__(self, __id, component, description, start_time, end_time, owner, emails):
        """ Constructor """
        self._id = __id
        self._component = component
        self._description = description
        self._start_time = start_time
        self._end_time = end_time
        self._owner = owner
        self._emails = emails

    @property
    def id(self):
        return self._id

    @property
    def component(self):
        return self._component

    @property
    def description(self):
        return self._description

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def owner(self):
        return self._owner

    @property
    def emails(self):
        return self._emails

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        """Overrides the default implementation (unnecessary in Python 3)"""
        return not self.__eq__(other)

    def __str__(self):
        """ Represent the object as a string """
        return "id: {0}, component: {1}, description: {2}, start time: {3}, end_time {4}, owner: {5}, emails: {6}".\
            format(self._id, self._component, self._description, self._start_time, self._end_time,
                   self._owner, self._emails)

    def __hash__(self):
        """ Hash the object based on an id """
        return hash(self._id)

    def check(self):
        """
        Method to check the validity of a request
        :return: In case there is an error, False will be returned. If all OK, True will be returned
        """
        if not self._description or not self._description or not self._start_time \
                or not self._end_time or not self.check_mail_addresses():
            return False
        else:
            return True

    def check_mail_addresses(self):
        """ Check the validity of the email addresses """
        pattern = re.compile(r"\"?([-a-zA-Z0-9.`?{}]+@\w+\.\w+)\"?")
        result = True
        for email in self._emails.split(","):
            if not email.strip():
                result = False
            elif not re.match(pattern, email):
                result = False
        return result

