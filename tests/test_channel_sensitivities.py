import unittest
from plugins.channel_sensitivities import CachedSensitivityData
import pandas as pd
import numpy as np

MODULE = "s62442.DL_RF_SENSORS.DLPA0_RFGEN1_MODX_REFLECT_PWR{{module:1A}}'"


class ChannelSensitivitiesTest(unittest.TestCase):
    """
    Class that unittests the channel_sensitvity plugin. It tests the calculation of the
    variations of the sensitivity signals.
    """

    dt_safety_start = pd.Timestamp(year=2018, month=6, day=2, hour=5).tz_localize('UTC')

    @classmethod
    def setUpClass(cls):
        CachedSensitivityData()
        CachedSensitivityData().set_dt_safety_start(ChannelSensitivitiesTest.dt_safety_start)

    @classmethod
    def setUp(cls):
        # Reset the internal dataframe
        CachedSensitivityData().set_df_sensitivities(pd.DataFrame())
        CachedSensitivityData().set_dt_safety_start(ChannelSensitivitiesTest.dt_safety_start)

    @staticmethod
    def test_calculate_variation_three_days_of_data():

        # generate the following data
        # dt       5     11     17     23    5     11    17    23    5    11    17    23    5
        # value  100    101    102    103  104    105   106   107  108   109   110   111  112
        # AVG                     102.5                    106.5                  110.5
        # Variation: ((110.5 - 102.5) / 102.5) * 100.0))
        # Note: dt 5 is not included in the determination of the values!
        dt_end = ChannelSensitivitiesTest.dt_safety_start + \
                 pd.Timedelta(days=3)
        for i, dt in enumerate(pd.date_range(
                start=ChannelSensitivitiesTest.dt_safety_start,
                end=dt_end,
                freq="6H")):
            CachedSensitivityData().add_sensitivity_signal(MODULE, 100.0 + i, dt)
        assert(np.isclose(CachedSensitivityData().calculate_variation(dt_end, MODULE),
                          ((110.5 - 102.5) / 102.5) * 100.0))

    @staticmethod
    def test_calculate_variation_less_than_three_days_of_data():

        # generate the following data
        # dt       5     11     17     23    5     11    17    23    5    11    17    23    5
        # value    x      x    100    101  102    103   104   105  106   107   108   109  110
        # AVG                         101                  104.5                  108.5
        # Variation: ((108.5 - 101) / 101) * 100.0))
        dt_end = ChannelSensitivitiesTest.dt_safety_start + \
                 pd.Timedelta(days=3)
        for i, dt in enumerate(pd.date_range(
                start=ChannelSensitivitiesTest.dt_safety_start + pd.Timedelta(hours=12),
                end=dt_end,
                freq="6H")):
            CachedSensitivityData().add_sensitivity_signal(MODULE, 100.0 + i, dt)
        assert(np.isclose(CachedSensitivityData().calculate_variation(dt_end, MODULE),
                          ((108.5 - 101) / 101) * 100.0))

    @staticmethod
    def test_calculate_variation_exactly_two_days_of_data():

        # generate the following data
        # dt       5     11     17     23    5     11    17    23    5    11    17    23    5
        # value    x      x      x      x    x    100   101   102  103   104   105   106  107
        # AVG                                             101.5                  105.5
        # Variation: ((105.5 - 101.5) / 101.5) * 100.0))
        dt_end = ChannelSensitivitiesTest.dt_safety_start + \
                 pd.Timedelta(days=3)
        for i, dt in enumerate(pd.date_range(
                start=ChannelSensitivitiesTest.dt_safety_start + pd.Timedelta(hours=30),
                end=dt_end,
                freq="6H")):
            CachedSensitivityData().add_sensitivity_signal(MODULE, 100.0 + i, dt)
        assert(np.isclose(CachedSensitivityData().calculate_variation(dt_end, MODULE),
                          ((105.5 - 101.5) / 101.5) * 100.0))

    @staticmethod
    def test_calculate_variation_less_than_two_days_of_data_but_two_averages():

        # generate the following data
        # dt       5     11     17     23    5     11    17    23    5    11    17    23    5
        # value    x      x      x      x    x      x   100   101  102   103   104   105  106
        # AVG                                              101                   104.5
        # Variation: ((104.5 - 101) / 101) * 100.0))
        dt_end = ChannelSensitivitiesTest.dt_safety_start + \
                 pd.Timedelta(days=3)
        for i, dt in enumerate(pd.date_range(
                start=ChannelSensitivitiesTest.dt_safety_start + pd.Timedelta(hours=36),
                end=dt_end,
                freq="6H")):
            CachedSensitivityData().add_sensitivity_signal(MODULE, 100.0 + i, dt)
        assert(np.isclose(CachedSensitivityData().calculate_variation(dt_end, MODULE),
                          ((104.5 - 101) / 101) * 100.0))

    @staticmethod
    def test_calculate_variation_less_than_two_days_of_data_just_two_averages():

        # generate the following data
        # dt       5     11     17     23    5     11    17    23    5    11    17    23    5
        # value    x      x      x      x    x      x     x     x  100   101   102   103  104
        # AVG                                               100                  102.5
        # Variation: ((104.5 - 101) / 101) * 100.0))
        dt_end = ChannelSensitivitiesTest.dt_safety_start + \
                 pd.Timedelta(days=3)
        for i, dt in enumerate(pd.date_range(
                start=ChannelSensitivitiesTest.dt_safety_start + pd.Timedelta(hours=48),
                end=dt_end,
                freq="6H")):
            CachedSensitivityData().add_sensitivity_signal(MODULE, 100.0 + i, dt)
        assert(np.isclose(CachedSensitivityData().calculate_variation(dt_end, MODULE),
                          ((102.5 - 100) / 100) * 100.0))

    @staticmethod
    def test_calculate_variation_exactly_one_day_of_data_four_data_points():

        # generate the following data
        # dt       5     11     17     23    5     11    17    23    5    11    17    23    5
        # value    x      x      x      x    x      x     x     x    x   101   102   103  104
        # AVG                                                                    102.5
        # Variation: N/A
        dt_end = ChannelSensitivitiesTest.dt_safety_start + \
                 pd.Timedelta(days=3)
        for i, dt in enumerate(pd.date_range(
                start=ChannelSensitivitiesTest.dt_safety_start + pd.Timedelta(hours=54),
                end=dt_end,
                freq="6H")):
            CachedSensitivityData().add_sensitivity_signal(MODULE, 100.0 + i, dt)
        assert(np.isnan(CachedSensitivityData().calculate_variation(dt_end, MODULE)))

    @staticmethod
    def test_calculate_variation_two_averages_in_two_days():

        # generate the following data
        # day      2                         3                       4                      5
        # dt       5     11     17     23    5     11    17    23    5    11    17    23    5
        # value    x      x      x      x    x      100   x     x    x     x     x     x  101
        # AVG                                                      100                    101
        # Variation:
        CachedSensitivityData()\
            .add_sensitivity_signal(
            MODULE,
            100.0,
            pd.Timestamp(year=2018, month=6, day=3, hour=11).tz_localize('UTC'))
        dt_end = pd.Timestamp(year=2018, month=6, day=5, hour=5).tz_localize('UTC')
        CachedSensitivityData()\
            .add_sensitivity_signal(
            MODULE,
            101.0, dt_end)
        assert(np.isclose(CachedSensitivityData().calculate_variation(dt_end, MODULE),
                          ((101 - 100.0) / 100.0) * 100.0))

    @staticmethod
    def test_calculate_variation_just_one_average_in_two_days():

        # generate the following data
        # day      2                         3                       4                      5
        # dt       5     11     17     23    5     11    17    23    5    11    17    23    5
        # value    x      x      x      x  100      x     x     x    x     x     x     x  101
        # AVG                                                      n/a                    101
        # Variation:
        CachedSensitivityData()._dt_safety_start = \
            pd.Timestamp(year=2018, month=6, day=3, hour=11).tz_localize('UTC')
        CachedSensitivityData()\
            .add_sensitivity_signal(
            MODULE,
            100.0, pd.Timestamp(year=2018, month=6, day=3, hour=5).tz_localize('UTC'))
        dt_end = pd.Timestamp(year=2018, month=6, day=5, hour=5).tz_localize('UTC')
        CachedSensitivityData()\
            .add_sensitivity_signal(
            MODULE,
            101.0, dt_end)
        assert(np.isnan(CachedSensitivityData().calculate_variation(dt_end, MODULE)))


if __name__ == '__main__':
    unittest.main()
