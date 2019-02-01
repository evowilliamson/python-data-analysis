from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, String, Boolean
import datetime
from tools.ondemand.config import DELETED


class Request(declarative_base()):
    """ This class represents the ORM for the Request entity """

    __tablename__ = 'request'
    id = Column(String, primary_key=True)
    description = Column(String(255))
    component = Column(String(255))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    emails = Column(String(1000))
    validated = Column(Boolean)
    owner = Column(String(255))
    last_updated = Column(DateTime)
    update_action = Column(String(255))

    def update(self, other, validated, action):
        """
        This method updates the fields from the other object to the self object
        :param other: the source object from which fields are updated
        :param validated: if the request was validated correctly true, else false
        :param action: the action, which can be DELETED, UPDATED or NEW
        :return:
        """
        self.last_updated = datetime.datetime.now()
        if action == DELETED:
            self.update_action = action
        else:
            self.description = other.description
            self.owner = other.owner
            self.start_time = other.start_time
            self.end_time = other.end_time
            self.emails = other.emails
            self.component = other.component
            self.validated = validated
            self.update_action = action + ": Old values: " + str(other)

    def __str__(self):
        """ Represent the object as a string """
        return "id: {0}, component: {1}, description: {2}, start time: {3}, end_time {4}, owner: {5}, emails: {6}".\
            format(self.id, self.component, self.description, self.start_time, self.end_time,
                   self.owner, self.emails)
