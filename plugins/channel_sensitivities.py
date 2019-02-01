__author__ = "Martijn Heuzinkveld (MHFC), Ivo Willemsen (IWIO)"
__date__ = "2018-06-06"

import datetime
import pandas as pd
import numpy as np
import statsmodels.formula.api as sm
import shared as sh
from shared import Singleton

USER = 'admin'
LOGGER = sh.get_logger(sh.TASK_LOG)

AVG_SIGNAL_SUB_PATTERN = "_REFLECT_PWR_AVERAGE"
RAW_SIGNAL_SUB_PATTERN = "_RFGEN"
AVG_SIGNALS = ["DL_RF_SENSORS.DLPA{0}_REFLECT_PWR_AVERAGE".format(s) for s in range(0, 3)]
ALL_AVG_SIGNAL = "DL_RF_SENSORS.DLPA0123" + AVG_SIGNAL_SUB_PATTERN
RAW_SIGNAL_PATTERN = "DL_RF_SENSORS.DLPA{0}" + RAW_SIGNAL_SUB_PATTERN + "{1}_MODX_REFLECT_PWR{2}"
SENSITIVITY_SIGNAL_PATTERN = "DL_RF_SENSORS.DLPA{0}" + RAW_SIGNAL_SUB_PATTERN + \
                      "{1}_MODX_REFLECT_PWR_Sens{2}"
MODULE_LEVEL_NAME = "REFLECT_PWR"
UTC_TZ = 'UTC'
TIMES_PER_DAY = 4
EXTRA_SAFETY_DAYS_SENSITIVITY = 5

LINEAR_FIT = 1


class CachedSensitivityData:
    __metaclass__ = Singleton
    """
    Class used to maintain sensitivity related information in order not to move
    around any data over all the functions
    """

    def __init__(self):
        """
        Constructor
        """
        self._dt_safety_start = 0
        self._df_sensitivities = pd.DataFrame()

    def reset_sensitivities(self):
        """
        Resets the dataframe for usage by another machine
        :return:-
        """
        self._df_sensitivities = pd.DataFrame()

    def add_extra_safety_sensitivities(self, df_sensitivities):
        """
        Function to add safety sensitivity signal. In case not a minimum of three
        data points can be found when determining the sensitivity variation, an safety
        lookup is done in order to determine a next best approach
        :param df_sensitivities: dataframe with sensitivities
        """
        self._df_sensitivities = df_sensitivities

    def add_sensitivity_signal(self, module, signal, datapoint_dt):
        """
        Adds a sensitivity signal to the internal data structures of sensitivities
        :param module: the module for which the signal must be added
        :param signal: the signal value itself
        :param datapoint_dt: the timestamp to be used as an index
        :return:-
        """
        module = self.__resolve_module_for_sens(module)
        self._df_sensitivities = self._df_sensitivities.append(
            pd.DataFrame(
                [[signal]],
                index=[pd.Timestamp(datetime.datetime(
                    datapoint_dt.year, datapoint_dt.month, datapoint_dt.day, datapoint_dt.hour))
                           .tz_localize('UTC')],
                columns=[module]))

    def calculate_variation(self, datapoint_dt, module):
        """
        Calculate the variation of the last three days. If there are not three data points
        in the last three days, than go back until at least two data points are found
        from of ${_start_dt_back_crawl} until ${datapoint_dt}
        :param datapoint_dt:
        :param module:
        :return:
        """

        module = self.__resolve_module_for_sens(module)
        try:
            self._df_sensitivities[module]
        except KeyError:
            return np.nan

        df_sensitivities = self.__get_three_data_points_last_three_days(module, datapoint_dt)
        if df_sensitivities.empty:
            df_sensitivities = self.\
                __get_two_data_points_from_dt_safety_start(module, datapoint_dt)
            if df_sensitivities.empty:
                return np.nan

        return ((df_sensitivities[df_sensitivities.columns[0]].max() -
                 df_sensitivities[df_sensitivities.columns[0]].min()) /
                df_sensitivities[df_sensitivities.columns[0]].min()) * 100

    def __get_three_data_points_last_three_days(self, module, datapoint_dt):
        """
        This function retrieves the last three data points from the last three days
        :param module: the module
        :param datapoint_dt: the end data point
        :return: the requested data frame
        """
        return self.__get_sensitivities_date_range(
            module, datapoint_dt - pd.Timedelta(days=3), datapoint_dt, 3)

    def __get_two_data_points_from_dt_safety_start(self, module, datapoint_dt):
        """
        This function retrieves data from the input data frame from the safety_start date.
        Safety start date is used in case it is not possible to find three data points in the
        last three days. In that case, two data points need to be found, basically ignoring all
        days without data points and retrieving the last two data points
        :param module: the module
        :param datapoint_dt: the end data point
        :return: data frame with requested data
        """
        return self.__get_sensitivities_date_range(
            module, self._dt_safety_start, datapoint_dt, 2)

    def __get_sensitivities_date_range(self, module, dt_start, dt_stop, n_o_days):
        """
        This function averages the values in the dataframe per day. In case there is no
        average, the nans will be skipped from the dataframe. It will give back the
        final {n_o_days} rows in the dataframe

        :param module: the module
        :param dt_start: start timestamp
        :param dt_stop: stop timestamp
        :param n_o_days: the number of days
        :return:
        """

        _signal = "signal"
        # Skip the first signal, not part of 24h window because dt_stop is inclusive
        dt_start = dt_start + pd.Timedelta(hours=24/TIMES_PER_DAY)
        date_range = self._df_sensitivities[module].sort_index(
            inplace=False).loc[dt_start:dt_stop].dropna()
        df_final_result = pd.DataFrame()
        for n_start in pd.date_range(start=dt_start, end=dt_stop, freq="D"):
            n_stop = n_start + pd.Timedelta(hours=24 - (24/TIMES_PER_DAY))
            date_range_sorted = date_range.sort_index(inplace=False)
            _mean = date_range_sorted.loc[n_start:n_stop].mean()
            if np.isnan(_mean):
                continue
            df_final_result = df_final_result.append(pd.DataFrame(
                [[_mean]],
                index=[n_stop],
                columns=[_signal]))

        if len(df_final_result) < n_o_days:
            return pd.DataFrame()
        return df_final_result.tail(n_o_days)

    @staticmethod
    def __resolve_module_for_sens(module):
        """
        Resolves module tag related characters
        :param module: the module
        :return: the resolves module
        """
        return module.replace("{{", "{").replace("}}", "}").replace(":", "=")\
            .replace("_MODX_REFLECT_PWR", "_MODX_REFLECT_PWR_Sens")

    @property
    def df_sensitivities(self):
        return self._df_sensitivities

    def set_df_sensitivities(self, df_sensitivities_):
        self._df_sensitivities = df_sensitivities_
        
    def set_dt_safety_start(self, dt_safety_start_):
        self._dt_safety_start = dt_safety_start_


def calculate_channel_sensitivities(name, machines=None, days_back=5):
    """
    Calculates the channel sensitivities and variations
    :param machines: a list of machines
    :param days_back: the number of days back that should be crawled
    :return:-
    """

    if not machines:
        machines = sh.Config().get_machines()

    dt_stop = calc_stop_time_at_day_interval(get_now(), TIMES_PER_DAY)
    CachedSensitivityData()
    CachedSensitivityData().set_dt_safety_start(
        dt_stop - pd.Timedelta(days=days_back + EXTRA_SAFETY_DAYS_SENSITIVITY))

    for machine in machines:
        calculate_machine_channel_sensitivities(days_back, machine, dt_stop)


def calculate_machine_channel_sensitivities(days_back, machine, dt_stop):
    """
    Calculates channel sensitivities and variations for all days for a certain machine
    :param days_back: the number of days back that should be crawled
    :param machine: the machine
    :param dt_stop: stop datetime, closest 6 hour moment
    :return: -
    """

    dt_start = dt_stop - pd.Timedelta(days=days_back)
    df_raw_signals = get_signals(machine, RAW_SIGNAL_PATTERN, dt_start, dt_stop)
    df_avg_signals = get_averages_signals(machine, dt_start, dt_stop)
    df_avg_all_signals = get_all_averages_signal(machine, dt_start, dt_stop)
    LOGGER.info("Calculate Channel Sensitivities: #signals (raw/avg/all_avg) " +
                str(len(df_raw_signals)) + "/" + str(len(df_avg_signals)) + "/" +
                str(len(df_avg_all_signals)))

    # Get Sensitivity signals, take EXTRA_DAYS_BACK_SENSITIVITY days extra,
    # as these are possibly needed to calculate the variations for the current day.
    # Store the result in a singleton.
    CachedSensitivityData().reset_sensitivities()
    CachedSensitivityData().add_extra_safety_sensitivities(
        get_signals(
            machine, SENSITIVITY_SIGNAL_PATTERN,
            dt_stop - pd.Timedelta(days=days_back + EXTRA_SAFETY_DAYS_SENSITIVITY), dt_stop))
    df_final_results = pd.DataFrame()
    for dt_start in pd.date_range(start=dt_start,
                                  end=dt_stop - pd.Timedelta(hours=24),
                                  freq="6H"):
        dt_stop = dt_start + pd.Timedelta(hours=24)
        df_final_results = process_time_window(
            df_final_results=df_final_results, df_avg_all_signals=df_avg_all_signals,
            df_avg_signals=df_avg_signals, df_raw_signals=df_raw_signals,
            dt_start=dt_start, dt_stop=dt_stop)

    sh.save_to_db(df_final_results, None)


def process_time_window(df_final_results, df_avg_all_signals, df_avg_signals, df_raw_signals,
                        dt_start, dt_stop):
    """
    Calculates sensitivities and variations for one machine for one day
    :param df_final_results: the dataframe that will contain final results
    :param df_avg_all_signals: dataframe that contains the overall averages
    :param df_avg_signals: dataframe that contains the averages per DLPA
    :param df_raw_signals: dataframe that contains the raw signals
    :param dt_start: start date
    :param dt_stop: end date
    :return: df_final_results
    """

    df_day_avg_all_signals = df_avg_all_signals[dt_start:dt_stop]
    df_day_avg_signals = df_avg_signals[dt_start:dt_stop]
    df_day_raw_signals = df_raw_signals[dt_start:dt_stop]

    if df_day_raw_signals.empty:
        return df_final_results

    for dlpa in range(0, 4):
        df_dlpa_avg_signals = \
            df_day_avg_signals.filter(regex="DLPA" + str(dlpa) + AVG_SIGNAL_SUB_PATTERN)
        df_dlpa_raw_signals = \
            df_day_raw_signals.filter(regex="DLPA" + str(dlpa) + RAW_SIGNAL_SUB_PATTERN)
        df_final_results = process_dlpa(
            df_final_results=df_final_results, df_avg_all_signals=df_day_avg_all_signals,
            df_avg_signals=df_dlpa_avg_signals, df_raw_signals=df_dlpa_raw_signals,
            datapoint_dt=dt_stop)
    return df_final_results


def process_dlpa(df_final_results, df_avg_all_signals, df_avg_signals, df_raw_signals,
                 datapoint_dt):
    """
    Does the processing for a certain DLPA
    :param df_final_results: the dataframe that will contain final results
    :param df_avg_all_signals: dataframe that contains the overall averages
    :param df_avg_signals: dataframe that contains the averages per DLPA
    :param df_raw_signals: dataframe that contains the raw signals
    :param datapoint_dt: datetime of the datapoint to be generated
    :return: df_final_results
    """

    for module in df_raw_signals:
        df_final_results = process_module(
            df_final_results=df_final_results, df_avg_all_signals=df_avg_all_signals,
            df_avg_signals=df_avg_signals, df_raw_signals=df_raw_signals,
            module=module, datapoint_dt=datapoint_dt)
    return df_final_results


def process_module(df_final_results, df_avg_all_signals, df_avg_signals, df_raw_signals,
                   module, datapoint_dt):
    """
    Fits the data
    :param df_final_results: the dataframe that will contain final results
    :param df_avg_all_signals: dataframe that contains the overall averages
    :param df_avg_signals: dataframe that contains the averages per DLPA
    :param df_raw_signals: dataframe that contains the raw signals
    :param module: the module
    :param datapoint_dt: stop date
    :return:
    """

    sanity_check = sum(x > 1 for x in df_raw_signals[module])
    if sanity_check < 100:
        return df_final_results

    # Perform the linear regression
    model_single = sm.OLS(df_raw_signals[module], df_avg_signals, missing='drop').fit()
    model_avg_all = sm.OLS(df_raw_signals[module], df_avg_all_signals, missing='drop').fit()
    module = module.replace("{", "{{").replace("}", "}}").replace("=", ":")

    CachedSensitivityData().add_sensitivity_signal(
        module=module, signal=model_single.params[-1], datapoint_dt=datapoint_dt)

    df_final_results = df_final_results.append(
        pd.DataFrame(
            [[model_single.params[-1],
              model_avg_all.params[-1],
              model_single.rsquared,
              model_avg_all.rsquared,
              CachedSensitivityData().calculate_variation(datapoint_dt, module)]],
            index=[pd.Timestamp(datetime.datetime(
                datapoint_dt.year, datapoint_dt.month, datapoint_dt.day, datapoint_dt.hour))
                       .tz_localize('UTC')],
            columns=[get_module_signal_name(module, "Sens"),
                     get_module_signal_name(module, "Sens_4PA"),
                     get_module_signal_name(module, "R"),
                     get_module_signal_name(module, "R_4PA"),
                     get_module_signal_name(module, "Sens_VAR")]
        ))

    return df_final_results


def calc_stop_time_at_day_interval(ts_stop, times_per_day):
    """
    Calculate the stop time rounded at the frequency interval
    Let's say it's now 15:30. Then it will give back 12

    :param ts_stop:
    :param times_per_day:

    :return: the stop time
    """

    stop = 0
    for _i in range(0, 24, 24 / times_per_day):
        if _i > ts_stop.hour:
            break
        stop = _i
    return pd.Timestamp(datetime.datetime(ts_stop.year, ts_stop.month, ts_stop.day, stop))\
        .tz_localize("UTC")


def get_module_signal_name(module, _id):
    """
    Determines the name of the signal at the module level
    :param module: name of the module
    :param _id: the signal identifier
    :return: the name of the signal at the module level
    """

    return module.replace(MODULE_LEVEL_NAME, MODULE_LEVEL_NAME + "_" + _id)


def get_signals(machine, pattern, dt_start, dt_stop):
    """
    Get raw signals from the database
    :param machine: the machine
    :param pattern: the pattern that will be used to retrieve the signals
    :param dt_start: start date
    :param dt_stop: end date
    :return: dataframe with the results
    """

    signals = ["s" + str(machine['source_nr']) + '.' +
               r for r in [pattern
                           .format(str(pa), str(gen), "{module=" + str(mod) + ab + "}")
               for pa in range(0, 4) for gen in range(1, 3) for mod in range(1, 9)
               for ab in ["A", "B"]]]
    return sh.get_signals(signals, USER, from_time=dt_start, to_time=dt_stop)


def get_all_averages_signal(machine, dt_start, dt_stop):
    """
    Get overall average signal from the database
    :param machine: the machine
    :param dt_start: start date
    :param dt_stop: end date
    :return: dataframe with the results
    """

    return sh.get_signals(["s" + str(machine['source_nr']) + '.' + ALL_AVG_SIGNAL], USER,
                          from_time=dt_start, to_time=dt_stop)


def get_averages_signals(machine, dt_start, dt_stop):
    """
    Get averages for all DLPA's
    :param machine: the machine
    :param dt_start: start date
    :param dt_stop: end date
    :return: dataframe with the results
    """

    signals = ["s" + str(machine['source_nr']) + '.' +
               a for a in AVG_SIGNALS]
    return sh.get_signals(signals, USER, from_time=dt_start, to_time=dt_stop)


def get_now():
    """
    Gets time current time in UTC tz
    :return: the current time in UTC tz
    """

    return pd.Timestamp(pd.datetime.now()).tz_localize(UTC_TZ)
