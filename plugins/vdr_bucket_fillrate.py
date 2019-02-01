import numpy as np
import pandas as pd
import shared as sh
import datetime as dt

# Fill rates should be generated for the following GPs (first tuple value)
# and GP conditioning sizes (second tuple value)
GP_CONDITIONS = [(5, 2), (10, 7), (20, 15)]
# user to access db
USER = "admin"
# the minimum number of hourly buckets for 24h medium calculation
MIN_HOURLY_BUCKETS = 7
# Constant used to indicate lineair fit when calling np.polyfit
LINEAR_FIT = 1
# Overwrite options
NEW_DATA_ONLY = "new_data_only"
# Time zone definition
UTC_TZ = "UTC"
# Task logger used throughout the program
TASK_LOGGER = sh.get_logger(sh.TASK_LOG)
BOTTOM = "bot"
TOP = "top"
SOURCE_NR = "source_nr"
TEMP = "temp"
HOUR = "hour"
MIN_TEMP = 547.15
MAX_TEMP = 549.15
FILL_LEVEL_2 = "VDR_BUCKET._FillLevel_2"
FILL_LEVEL_MEDIAN_24H = "VDR_BUCKET._FillLevel_Median_24h"
PULSECOUNT_HTVB_LEVEL_MEDIAN_24H = "VDR_BUCKET._PulseCount_HTVBLevel_Median_24h"
MEDIAN_FILL_LEVEL = "median_fill_level"
PULSECOUNT = "pulse_count"
PULSECOUNT_INTERPOLATED = "pulsecount_interpolated"


def task_vdr_bucket_fill_rate(name, machines=None, days_back=5):
    """
    Calculates the fill rates by looking back X GP Pulsecounts

    :param name: The name of the task
    :param machines: a list of machines for which the calculation should be done
    :param days_back: The 'number of days' of data that should be written to the database

    :return:-
    """

    TASK_LOGGER.info("Start: Fill Rate ({0}), days_back = {1}".format(name, days_back))
    # for machine in (machines if machines else sh.Config().get_machines()):
    for machine in get_machines(machines):
        machine_nr = "s" + str(machine[SOURCE_NR])
        TASK_LOGGER.info("Start: Fill Rate calculation for machine " + machine_nr)
        collector_intervals = get_collector_intervals(machine_nr, days_back)
        if not collector_intervals.empty:
            process_collector_swaps(
                machine, collector_intervals, days_back)
        TASK_LOGGER.info("Done: Fill Rate calculation for machine " + machine_nr)
    TASK_LOGGER.info("Done: Fill Rate")


def get_collector_intervals(machine_nr, days_back):
    """
    Returns a list of collector names and a list swap_time interval tuples

    :param machine_nr: Machine number
    :param days_back: The number of days to consider for going back in swap_time

    :return: list of collector intervals and a list of names of collector names
    """

    try:
        signal = machine_nr + ".Collector.Swap"
        data = sh.get_signals(signal, USER)
        ts_start = get_now() - dt.timedelta(days=days_back)
        first = True
        for swap_time in reversed(data.index):
            if (swap_time > ts_start) or (ts_start >= swap_time and first):
                if ts_start >= swap_time and first:
                    first = False
            else:
                data.drop(swap_time, inplace=True)
        if data.empty:
            # If no swaps have been defined
            data = pd.concat([data,
                              pd.DataFrame([["None"]],
                                           columns=[signal],
                                           index=[pd.Timestamp(dt.datetime.fromtimestamp(0 / 1000000)).
                                           tz_localize(UTC_TZ)])], ignore_index=False)
        data = pd.DataFrame(pd.concat([data, pd.DataFrame([["None"]], columns=[signal], index=[get_now()])],
                            ignore_index=False))
        return data.sort_index()
    except Exception as e:
        TASK_LOGGER.error("Fill Rate: Error retrieving collectors: " + e.message + " Machine: " +
                          machine_nr)
        return pd.DataFrame()


def get_machines(machines):
    """
    Get all machines, except version S1 machines
    :param machines: the list of machines
    :return: list of filtered machines
    """

    result_machines = []
    for machine in (machines if machines else sh.Config().get_machines()):
        if machine["version"] != "S1":
            result_machines.append(machine)
    return result_machines


def process_collector_swaps(machine, collector_intervals, days_back):
    """
    Perform the calculations for each collector/swap

    :param machine: the machine for which the calculations should be performed
    :param collector_intervals: dataframe with collector intervals
    :param days_back: the number of days that should be considered for removing old data

    :return:-
    """

    source_nr = "s" + str(machine[SOURCE_NR])
    collector_name = collector_intervals.iloc[0][0]
    start_time = collector_intervals.index[0]
    for i in range(1, len(collector_intervals)):
        end_time = collector_intervals.index[i]
        TASK_LOGGER.info("Start: Fill Rate calculation for machine {0}, collector {1}, "
                         "start time {2}, end time {3}"
                         .format(source_nr, collector_name, start_time, end_time))

        try:
            if calc_vdr_bucket_fill_level2(machine=machine, days_back=days_back, ts_start=start_time, ts_stop=end_time):
                calc_vdr_bucket_medians(machine=machine, days_back=days_back, ts_start=start_time, ts_stop=end_time)
                df = get_base_signals(machine=machine, collector_name=collector_name,
                                      ts_start=start_time, ts_stop=end_time)
                if not df.empty:
                    calculate_fill_rates(machine, join_interpolate(df), start_time, days_back)
        except Exception as e:
            TASK_LOGGER.error(
                "Error during Fill Rate calculation {0} [{1}]. {2}".format(source_nr, collector_name, str(e)))

        TASK_LOGGER.info(
            "Done: Fill Rate calculation for machine {0}, collector {1}".format(source_nr, collector_name))
        start_time = end_time
        collector_name = collector_intervals.iloc[i][0]


def calc_vdr_bucket_fill_level2(machine, ts_start, ts_stop, days_back):
    """
    Calculates vdr bucket fill level 2

    :param machine: machine dict
    :param ts_start: Start time of the calculation
    :param ts_stop:  Stop time of the calculation
    :param days_back: the number of days that should be considered for retrieving and removing old data

    :return: True if data was returned in one of the data frames, False if not
    """

    source_nr = "s{0}".format(machine[SOURCE_NR])

    # Get the max: either the collector start time or the now - days_back (+ partial day)
    ts_start = max(get_now() - dt.timedelta(days=(days_back + 1)), ts_start)
    df1 = get_temperatures_df("VDR_HEATER.TCbot", "VDR_HEATER.TCtop", source_nr, ts_start, ts_stop)
    df2 = get_temperatures_df("KPI.HTVB_Bot_Temperature_VALUE", "KPI.HTVB_Top_Temperature_VALUE",
                              source_nr, ts_start, ts_stop)
    df = pd.DataFrame(pd.concat([df1, df2]))
    if df.empty or len(df.columns) < 2:
        TASK_LOGGER.warning("Missing fill level base signals")
        return False
    df = df.loc[(df[BOTTOM] > MIN_TEMP) & (df[BOTTOM] < MAX_TEMP)]
    sh.save_to_db({source_nr + "." + FILL_LEVEL_2: 100 - (0.662 * (df[BOTTOM] - df[TOP])).astype(pd.np.float64)},
                  None, existing_data_option=NEW_DATA_ONLY)
    return True


def get_temperatures_df(bottom_signal, top_signal, source_nr, ts_start, ts_stop):
    """
    Gets the bottom and top temperatures from the databaes and add an extra "hour" column to
    it that trunks on the hour level for allowing group by on the hour later on

    :param bottom_signal: the name of bottom signal
    :param top_signal: the name of the top signal
    :param source_nr: the source number
    :param ts_start: start timestamp
    :param ts_stop: stop timestamp

    :return the created dataframe
    """

    df = sh.get_signals(
        [("{0}." + bottom_signal).format(source_nr), ("{0}." + top_signal).format(source_nr)], USER, ts_start, ts_stop)

    if df.empty or len(df.columns) < 2:
        return pd.DataFrame()
    elif len(df.columns) > 2:
        raise ValueError("Get signals returned more columns than expected")

    df.columns = [BOTTOM, TOP]
    df[TEMP] = df.index
    df[HOUR] = df[TEMP].apply(
        lambda x: pd.Timestamp(dt.datetime(
            year=x.year, month=x.month, day=x.day, hour=x.hour)).tz_localize(UTC_TZ))
    return df


def calc_vdr_bucket_medians(machine, ts_start, ts_stop, days_back):
    """
    Calculates the 24 hour median values of Collector._PulseCount and
    VDR_BUCKET._FillLevel2
    Note that days_back is not taken into consideration, as we need to look back in time
    for the pulse counts and median 24h level calculations

    :param machine: machine
    :param ts_start: Start time of the calculation
    :param ts_stop:  Stop time of the calculation
    :param days_back: For the save_to_db function ONLY, data is written from days_back days to ts_stop

    :return: True if data is found, False otherwise
    """

    source_nr = "s{0}".format(machine[SOURCE_NR])

    signal_fill_level_median_24h = ("{0}." + FILL_LEVEL_MEDIAN_24H).format(source_nr)
    signal_collector_pulsecount = "{0}.Collector._PulseCount".format(source_nr)
    signal_collector_pulsecount_median_24h = ("{0}." + PULSECOUNT_HTVB_LEVEL_MEDIAN_24H).format(source_nr)

    df_pc_median, df_fl_median = get_median_dfs(
        days_back=days_back, signal_collector_pulsecount=signal_collector_pulsecount,
        signal_collector_pulsecount_median_24h=signal_collector_pulsecount_median_24h,
        signal_fill_level_median_24h=signal_fill_level_median_24h,
        signal_fill_level2=("{0}." + FILL_LEVEL_2).format(source_nr),
        ts_start=ts_start, ts_stop=ts_stop)

    sh.save_to_db({
        signal_collector_pulsecount_median_24h: df_pc_median,
        signal_fill_level_median_24h: df_fl_median
    }, None)
    return


def get_median_dfs(days_back, signal_collector_pulsecount, signal_collector_pulsecount_median_24h,
                   signal_fill_level_median_24h, signal_fill_level2, ts_start, ts_stop):
    """
    Aggregation function that returns the calculted median pulsecount and fill level dataframes
    :param days_back: the number of days to crawl
    :param signal_collector_pulsecount: the name of the signal that represents the pulsecount
    :param signal_collector_pulsecount_median_24h: the name of the signal that represents the 24h-median pulsecount
    :param signal_fill_level_median_24h: the name of the signal that represents the 24h-median fill level signal
    :param signal_fill_level2: the name of the signal that represents the fill level2 signal
    :param ts_start: the start time
    :param ts_stop: the stop time
    :return: the two dataframes
    """

    base_signals_df = sh.get_signals([signal_fill_level2, signal_collector_pulsecount],
                                     USER, from_time=ts_start, to_time=ts_stop)
    if base_signals_df.empty or len(base_signals_df.columns) < 2:
        TASK_LOGGER.warning("Missing " + signal_fill_level2 + " or " + signal_collector_pulsecount)
        return None, None

    joined_df = get_interpolated_joined_pulsecount(base_signals_df, signal_collector_pulsecount, signal_fill_level2)

    pc_median_df = create_median_df(
        joined_df, PULSECOUNT_INTERPOLATED, signal_collector_pulsecount_median_24h,
        ts_start, ts_stop)[signal_collector_pulsecount_median_24h].astype(pd.np.float64)

    fl_median_df = create_median_df(
        base_signals_df, signal_fill_level2, signal_fill_level_median_24h,
        ts_start, ts_stop)[signal_fill_level_median_24h].astype(pd.np.float64)

    # Only save data to the database that is necessary, i.e. the max of either the collector start time or
    # the now - days_back (+ partial day + 1(24h range))
    ts_start_save = max(get_now() - dt.timedelta(days=(days_back + 2)), ts_start)
    return pc_median_df[pc_median_df.index > ts_start_save], fl_median_df[fl_median_df.index > ts_start_save]


def get_interpolated_joined_pulsecount(base_signals_df, signal_collector_pulsecount, signal_fill_level2):
    """
    Join the Fill Level and Collector pulse count in Base signal data frame and add a new column
    pulse_count_interpolated with
    the interpolated value of the collector pulse count
    :param base_signals_df: Base Signal data frame
    :param signal_collector_pulsecount: Name of the collector pulse count signal
    :param signal_fill_level2: Name of the Fill Level signal
    :return: Returns the data frame with interpolated value of the collector pulse count
    """
    fill_level2_df = base_signals_df[signal_fill_level2].to_frame(name=signal_fill_level2)
    joined_df = fill_level2_df.join(base_signals_df[signal_collector_pulsecount], how="outer")
    joined_df[PULSECOUNT_INTERPOLATED] = joined_df[signal_collector_pulsecount].interpolate(method="index")
    return joined_df


def get_base_signals(machine, collector_name, ts_start, ts_stop):
    """
    Gets the base signals that are required in order to calculate the fill rate
    Note that days_back is not taken into consideration, as we need to look back in time
    for the pulse counts and median 24h level

    :param machine: the machine
    :param collector_name: the name of the collector
    :param ts_start: start ime
    :param ts_stop: stop time

    :return: a dataframe that contains the data that pertains to the input signals
    """

    source_nr = "s" + str(machine[SOURCE_NR])
    # Get the Collector._PulseCount and VDR_HEATER._Fill_level_day signals
    signal_fld = ("s{0}." + FILL_LEVEL_MEDIAN_24H).format(machine[SOURCE_NR])
    signal_pc = ("s{0}." + PULSECOUNT_HTVB_LEVEL_MEDIAN_24H).format(machine[SOURCE_NR])
    base_signals_df = sh.get_signals([signal_fld, signal_pc], USER, from_time=ts_start, to_time=ts_stop)

    if base_signals_df.empty or len(base_signals_df.columns) < 2:
        TASK_LOGGER.warning("Fill Rate calculation: Missing {0} or {1} for machine {2}, collector {3}".
                            format(signal_fld, signal_pc, source_nr, collector_name))
        return pd.DataFrame()

    base_signals_df.columns = [MEDIAN_FILL_LEVEL, PULSECOUNT]
    return base_signals_df


def calculate_fill_rates(machine, rate_df, ts_start, days_back):
    """
    Calculate and stores the fill rates in the database

    :param machine: The machine
    :param rate_df: Data frame with fill levels and interpolated pulse counts
    :param ts_start: Start time of the calculation
    :param days_back: the number of days that should be considered for fitting and removing old data

    :return:-
    """

    # Get the max: either the collector start time or the now - days_back (+ partial day)
    ts_start = max(get_now() - dt.timedelta(days=(days_back + 1)), ts_start)

    for gp_cond in GP_CONDITIONS:
        rate_gp = rate_df[PULSECOUNT_INTERPOLATED][rate_df[PULSECOUNT_INTERPOLATED] > gp_cond[0]]. \
            apply(lambda x: fit_rate_pulsecount(x, rate_df, gp_cond))
        # Only keep 'days_back' days of data
        rate_gp = rate_gp[rate_gp.index > ts_start]
        sh.save_to_db({
                "s{0}.VDR_BUCKET._FillRate_{1}Gp".format(machine[SOURCE_NR], gp_cond[0]):
                rate_gp.astype(pd.np.float64)
            }, None)


def fit_rate_pulsecount(x, rate_df, gp_condition):
    """
    Performs a linear fit of interpolated pulsecounts and median fill levels

    :param x: the interpolated pulsecount
    :param rate_df: the dataframe that contains the fill level reates
    :param gp_condition: gp size and condition that should be applied

    :return: The slope of the fit
    """

    fitted_data_df = \
        rate_df[(rate_df[PULSECOUNT_INTERPOLATED] > (x - gp_condition[0])) & (rate_df[PULSECOUNT_INTERPOLATED] < x)]
    if np.size(fitted_data_df) == 0 \
            or (fitted_data_df[PULSECOUNT_INTERPOLATED][-1] -
                fitted_data_df[PULSECOUNT_INTERPOLATED][0]) < gp_condition[1]:
        return np.nan
    return pd.np.polyfit(fitted_data_df[PULSECOUNT_INTERPOLATED], fitted_data_df[MEDIAN_FILL_LEVEL], LINEAR_FIT)[0]


def join_interpolate(df):
    """
    Joins the fill levels with the interpolated pulse counts
    Eventhough interpolation of the Collector._PulseCount signal is done when the
    medians are calculated, it might be possible for medians PulseCount values not to be
    generated because of lack of signal. In that case, interpolation is necessary for
    missing median values. This action is thus necessary here

    :param df: dataframe with the input signals, being the fill levels and the pulse counts

    :return: dataframe with fill levels and interpolated pulse counts
    """

    rate_df = df[MEDIAN_FILL_LEVEL].to_frame(name=MEDIAN_FILL_LEVEL).join(df[PULSECOUNT], how="outer")
    # Interpolate to generate values for missing PulseCount medians
    rate_df[PULSECOUNT_INTERPOLATED] = rate_df[PULSECOUNT].interpolate(method="index") * 1e-9
    rate_df = rate_df[[MEDIAN_FILL_LEVEL, PULSECOUNT_INTERPOLATED]]
    rate_df.dropna(inplace=True)
    # don't calculate after last pulsecount:
    return rate_df[~rate_df[PULSECOUNT_INTERPOLATED].duplicated(keep="first")]


def calc_stop_time_at_day_interval(ts_stop, times_per_day):
    """
    Calculate the stop time rounded at the frequency interval
    Let's say it's now 15:30. Then it will give back 12

    :param ts_stop:
    :param times_per_day:

    :return: the stop time
    """

    stop = 0
    for i in range(0, 24, 24 / times_per_day):
        if i > ts_stop.hour:
            break
        stop = i
    return pd.Timestamp(dt.datetime(ts_stop.year, ts_stop.month, ts_stop.day, stop))\
        .tz_localize(UTC_TZ)


def create_median_df(df, signal_input, signal_output, ts_start, ts_stop):
    """
    Creates a dataframe with the median values based on the data in the df/signal_input
    It generates 4 medians per 24 hour at the hours 0, 6, 12 and 18 for the required days

    :param df: The dataframe with the input data
    :param signal_input: The input signal name
    :param signal_output: The output signal name
    :param ts_start: The start time
    :param ts_stop: The stop time

    :return: The generated data frame
    """

    # Generate the medians for the VDR_BUCKET._Fill_level2
    avg_df = pd.DataFrame()
    days_back = (ts_stop - ts_start).days
    times_per_day = 4
    stop_time = calc_stop_time_at_day_interval(ts_stop, times_per_day)
    df = df[signal_input]
    df.dropna(inplace=True)
    # Only days should be used of which there are datapoints in at least minimum_points
    # 1 hour buckets
    for i in range(times_per_day * days_back):
        start_time = stop_time - pd.Timedelta(hours=24)
        start_time = start_time + pd.Timedelta(seconds=1)
        if get_number_of_hourly_buckets_with_points(df, start_time, stop_time) >= MIN_HOURLY_BUCKETS:
            single_avg_df = pd.DataFrame(
                {"time": stop_time,
                 signal_output: {"value": df.loc[start_time:stop_time].median()}})
            single_avg_df = single_avg_df.set_index(["time"])
            avg_df = avg_df.append(single_avg_df)
        stop_time = stop_time - pd.Timedelta(hours=24/times_per_day)
    avg_df.dropna(inplace=True)
    avg_df.sort_index()
    return avg_df


def get_number_of_hourly_buckets_with_points(df, start_time, stop_time):
    """
    Get the number of hourly buckets with data points
    :param df: the dataframe with the data of the last 24 hour
    :param start_time: start time
    :param stop_time: stop time
    :return: number of hourly buckets with data points
    """
    df_24h = df.loc[start_time:stop_time]
    times = pd.DatetimeIndex(df_24h.index)
    grouped_cnt = df_24h.groupby([times.year, times.month, times.day, times.hour]).count()
    return len(grouped_cnt[grouped_cnt >= 1])


def get_now():
    """
    Gets time current time in UTC tz
    :return: the current time in UTC tz
    """

    return pd.Timestamp(pd.datetime.now()).tz_localize(UTC_TZ)
