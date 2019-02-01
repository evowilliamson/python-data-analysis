import time
import re
import datetime
from misc.singleton_metaclass import Singleton

# Constants used in this class
DEFAULT_TAG = "default"
RP_NAME_TAG = "name"
SHARD_GROUP_DURATION_TAG = "shardGroupDuration"
SPLIT_PATTERN = "h|m|s"
UNDEFINED_RP_SHARD_DURATION = -1
TIME = "time"
MEASUREMENT = "measurement"
SOURCE_NR = "source_nr"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class RetentionPolicyConfig:
    """ This Singleton class contains information about configured retention policies in all databases.
    This class cointains a dictionary of databases (key) and influx db retention policy response objects.
    It's also used to check data to be inserted in the database, against the retention policy in order to see
    if the data contains old data that might cause problems with compaction.

    It needs to be noted that eventhough a singleton is being used, workers and taskworkers are started as
    seperate Python programs, so they operate in their own VM. As a consequence, each worker and taskworker will
    have their own instance of RetentionPolicyConfi and thus execute (redudant) queries to retrieve retention
    policty information from the database. This pattern can be seen in more constructs in the application,
    like with the Mount or Translate 'Singleton' classes. This must be addressed in a future redesign of the
    application. """

    __metaclass__ = Singleton

    def __init__(self, dbclient, machines, logger):
        """
        Default constructor
        :param dbclient: the dbclient
        :param machines: the list of machines (i.e. databases [source])
        :param logger: the logger object
        """
        self.db_to_rp_mapping = {}
        self.logger = logger
        self.machines = machines
        self.dbclient = dbclient
        self.load()

    def load(self):
        """
        Method that loads the retention policies for all databases. When this method is being executed, any other
        method that is decorated with the indicated synchronization lock, will block
        :return:-
        """

        self.db_to_rp_mapping = {}
        for machine in self.machines:
            database = "s" + machine[SOURCE_NR]
            try:
                self.db_to_rp_mapping[database] = self.dbclient.get_list_retention_policies(database)
            except Exception as e:
                if "database not found" in e.message:
                    continue
                else:
                    raise e

    def check_data_age(self, database, retention_policy_name, job_name, data):
        """
        This method writes a log record (if conditions are met) stating that old data is being written
        to the database, which can cause compactions
        :param database: the database (source number)
        :param retention_policy_name: the name of the retention policy
        :param job_name the name of the job
        :param data: the data
        :return:-
        """

        if not data:
            return
        rp_name = retention_policy_name
        if not rp_name:
            # Get the default retention policy
            rp_name = self.get_default_retention_policy_name(database)
        rp_shard_group_duration = self.get_rp_shard_group_duration(database, rp_name)
        if rp_shard_group_duration == UNDEFINED_RP_SHARD_DURATION:
            return
        oldest_data_point = sorted(data, key=lambda k: k[TIME], reverse=False)[0]
        if oldest_data_point[TIME]/1000000000 + rp_shard_group_duration < time.time():
            self.logger.info("Possible compaction issue: Job {0}, is inserting a datapoint with timestamp {1}. "
                             "Signal is {2}, retention policy is {3} and shard group duration is {4} hours".
                             format(job_name,
                                    datetime.datetime.fromtimestamp(oldest_data_point[TIME]/1000000000).
                                    strftime(DATETIME_FORMAT), oldest_data_point[MEASUREMENT], rp_name,
                                    rp_shard_group_duration / 3600))
            return

    def get_retention_policies(self, database):
        """
        This method retrieves the retention policies for the database
        :param database: the database
        :return: the list of retention policies
        """
        return self.db_to_rp_mapping[database]

    def get_default_retention_policy_name(self, database):
        """
        Method that gets the name of the default retention policy of the database
        :param database: the database to be queried
        :return: the name of the default retention policy
        """
        for retention_policy in self.get_retention_policies(database):
            if retention_policy[DEFAULT_TAG]:
                name = retention_policy[RP_NAME_TAG]
                return name
        # If no default retention policy available, it should be considered as an error...
        return None

    def get_retention_policy(self, database, retention_policy_name):
        """
        The retention policy object that coincides with the name passed as a parameter, is retrieved
        :param database: the database for which the retention policy should be retrieved
        :param retention_policy_name: the name of the policy
        :return: the retention policy object
        """
        for retention_policy in self.get_retention_policies(database):
            if retention_policy[RP_NAME_TAG] == retention_policy_name:
                return retention_policy
        # If retention policy cannot be found, it should be considered as an error...
        return None

    def get_rp_shard_group_duration(self, database, retention_policy_name):
        """
        Method that gets the shard group duraction of the indicated retention polioy name and database.
        Influxdb returns the shard group duration in the format: <int>h<int>m<int>s. The object that is
        returned by Influxdb needs to be converted to seconds to be able to compare it later with the
        timestamp of the data points
        :param database: the database
        :param retention_policy_name: the name of the retention polioy
        :return: the shard group duration in seconds
        """

        retention_policy = self.get_retention_policy(database, retention_policy_name)
        if retention_policy:
            # If retention policy is found, return the shard group duration in seconds
            return self.get_shard_group_duration_in_seconds(database, retention_policy)
        else:
            self.logger.info("Retention policy for database {0} could not be determined, possibly wrong "
                             "retention policy name {1} has been provided, settting to max".
                             format(database, retention_policy_name))
            return UNDEFINED_RP_SHARD_DURATION

    def get_shard_group_duration_in_seconds(self, database, retention_policy):
        """
        This method returns the shard group duration given the database and retention policy.
        If for any reason, the shard group duration cannot be determined, MAX_RP will be returned, which will results
        in skipping the logging of possible data points that might have aged data
        :param database: the database
        :param retention_policy: the retention policy for which the shard group duration should be determined
        :return: the shard group duration in seconds.
        """
        rp_shard_group_duration = retention_policy[SHARD_GROUP_DURATION_TAG]
        if not rp_shard_group_duration:
            self.logger.info("Shard group duration for retention policy {0} for database {1} could not"
                             "be determined, setting to max".format(retention_policy[RP_NAME_TAG], database))
            return UNDEFINED_RP_SHARD_DURATION
        # format returned by influxdb: <int>h<int>m<int>s: needs to be splitted
        splitted = re.split(SPLIT_PATTERN, rp_shard_group_duration)
        if len(splitted) != 4:
            self.logger.info("Shard group duration {0} for retention policy {1} for database {1} has "
                             "incorrect format, setting to max".
                             format(rp_shard_group_duration, retention_policy[RP_NAME_TAG], database))
            return UNDEFINED_RP_SHARD_DURATION
        else:
            return (int(splitted[0]) * 60 * 60) + (int(splitted[1]) * 60)  # seconds are ignored in influxdb

