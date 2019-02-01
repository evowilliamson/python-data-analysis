import unittest
from misc.retention_policy_config \
    import RetentionPolicyConfig, SHARD_GROUP_DURATION_TAG, RP_NAME_TAG, DEFAULT_TAG, TIME, MEASUREMENT
from mock import MagicMock
from datetime import datetime
import time


class RetentionPolicyConfigTest(unittest.TestCase):
    """
    Class that unittests the RetentionPolicyConfig class. It uses MagicMock to mock the dbclient class
    """

    # remove singleton metaclass. For some reason, it doesn't work with the singleton metaclass attached to the class
    RetentionPolicyConfig.__metaclass__ = None

    def setUp(self):
        """
        This method sets up the tests by creating a RetentionPolicyConfig instance and mocking objects
        :return:-
        """

        self.source_nr = "62115"
        self.database = "s" + self.source_nr
        self.machines = [{"source_nr": self.source_nr}]
        self.rpc = RetentionPolicyConfig(
            dbclient=self.create_dbclient_mock(),
            machines=self.machines,
            logger=self.create_logger_mock())

    def test_constructor(self):
        """
        Simple checks
        :return:
        """
        self.assertEquals(self.rpc.machines, self.machines)
        self.assertEquals(len(self.rpc.db_to_rp_mapping), 1)  # number of databases

    def test_get_retention_policies(self):
        """
        Test that checks whether the get_retention_policies function retrieves the correct numer of retention policies
        :return:
        """
        self.assertEquals(len(self.rpc.get_retention_policies(self.database)), 2)

    def test_get_retention_policy(self):
        """
        Check whether the get_retention_policy retrieves the correct object based on a rp name
        :return:
        """
        rp = self.rpc.get_retention_policy(self.database, "rp1")
        self.assertEquals(rp[RP_NAME_TAG], "rp1")
        rp = self.rpc.get_retention_policy(self.database, "rp3")
        self.assertEquals(rp, None)

    def test_get_default_retention_policy_name(self):
        """
        Tests whether the correct default retention polioy is retrieved
        :return:
        """
        self.assertEquals(self.rpc.get_default_retention_policy_name(self.database), "rp1")

    def test_get_rp_shard_group_duration(self):
        """
        Checks to see if the correct shard group duration is retrieved
        :return:
        """
        rp = self.rpc.get_retention_policy(self.database, "rp1")
        self.assertEquals(self.rpc.get_shard_group_duration_in_seconds(self.database, rp), 6*60*60)

    def test_check_data_age_too_old(self):
        """
        Test that checks how the check_data_age behaves with data that is too old
        :return:
        """
        data = [{TIME: int(time.mktime(datetime(2018, 1, 21, 16, 30).timetuple())) * 1000000000, MEASUREMENT: 1.0},
                {TIME: int(time.mktime(datetime(2018, 1, 21, 16, 30).timetuple())) * 1000000000, MEASUREMENT: 2.0}]
        time.time = MagicMock(return_value=int(time.mktime(datetime(2018, 1, 21, 23, 30).timetuple())))
        self.rpc.check_data_age(self.database, "rp1", None, data)
        self.rpc.logger.info.assert_called()

    def test_check_data_age_ok(self):
        """
        Test that checks how the check_data_age behaves with data that is ok
        :return:
        """
        data = [{TIME: int(time.mktime(datetime(2018, 1, 21, 13, 30).timetuple())) * 1000000000, MEASUREMENT: 1.0},
                {TIME: int(time.mktime(datetime(2018, 1, 21, 12, 00).timetuple())) * 1000000000, MEASUREMENT: 2.0}]
        time.time = MagicMock(return_value=int(time.mktime(datetime(2018, 1, 21, 14, 30).timetuple())))
        self.rpc.check_data_age(self.database, "rp1", None, data)
        self.rpc.logger.info.assert_not_called()

    @staticmethod
    def create_logger_mock():
        """
        Static method that creates a logger mock
        :return: the mock
        """
        logger_mock = MagicMock()
        attrs = {'info.return_value': None}
        logger_mock.configure_mock(**attrs)
        return logger_mock

    @staticmethod
    def create_dbclient_mock():
        """
        Static method that creates the dbclient mock
        :return: the mock
        """
        dbclient_mock = MagicMock()
        attrs = {'get_list_retention_policies.return_value':
                 [{RP_NAME_TAG: "rp1", SHARD_GROUP_DURATION_TAG: "6h0m0s", DEFAULT_TAG: True},
                  {RP_NAME_TAG: "rp2", SHARD_GROUP_DURATION_TAG: "4h0m0s", DEFAULT_TAG: False}]}
        dbclient_mock.configure_mock(**attrs)
        return dbclient_mock


if __name__ == '__main__':
    unittest.main()
