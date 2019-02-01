from shared import Config, get_logger, TASK_LOG, get_redis_client, save_to_db
import arrow
import pandas as pd
from dbclients.DbClientFactory import DbClientFactory
from dbclients.DbClientConfig import DbClientConfig
from datetime import datetime, timedelta
import shared as sh

NAME = "name"
MACHINE_NR = "machine_nr"
SOURCE_NR = "source_nr"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
CHUNKSIZE = 10000
LOGGER = get_logger(TASK_LOG)
REDIS_CLIENT = get_redis_client()
PMA_CONFIG_ID = "pma"
PMA_FLAG = "use_pma_new"
PMA_QUERY = """
    SELECT
        fact.SAMPLE_DT AS "Timestamp",
        CAST(fact.VALUE AS FLOAT) AS "Value"
    FROM
          "PARAMETER_DIM" t, "PARAMETER_FACT" fact
    WHERE
          t.NAME = %s AND
          fact.PARAMETER_DIM_ID = t.PARAMETER_DIM_ID AND
          fact.EQUIPMENT_NR = %s AND fact.SAMPLE_DT > PARSETIMESTAMP(%s, 'yyyy-MM-dd hh:mm:ss')
    ORDER BY
          fact.SAMPLE_DT
    LIMIT %s"""

TODAY = datetime(datetime.utcnow().year, datetime.utcnow().month, datetime.utcnow().day, 0)

total_no_errors = 0
total_time_spent = 0.0


def task_pma_db_reader(name, machines=None, handler=None, days_back=None):
    """
    Function that encapsulates the reading of pma new data.
    :param name: the name of the job
    :param machines: the list of machines that should be processed
    :param handler: the handler
    :param days_back:
    :return:
    """

    LOGGER.info("Start: PMA DB reader, days_back = " + str(days_back))
    db_client = DbClientFactory.get_client(DbClientConfig.get(PMA_CONFIG_ID), LOGGER)

    signals, signal_type = get_signals_info(handler)
    if signals is None:
        return

    for signal in signals:
        print(signal["name"])

    for machine in get_machines(machines):
        LOGGER.info("PMA DB reader - Processing machine m" + machine[MACHINE_NR])
        for signal in signals:
            process_signal(
                db_client=db_client, handler=handler, machine=machine,
                signal=signal, signal_type=signal_type, job_name=name,
                days_back=days_back)

    db_client.close_connection()
    write_monitor_data()
    LOGGER.info("Finished: PMA DB reader")


def get_machines(machines):
    """
    Get all the machines that should be processed with pma new
    :param machines: the machiens
    :return: filtered list of machines ready to be read with pma new
    """

    if not machines:
        machines = Config().get_machines()
    return [m for m in machines if m[PMA_FLAG] == 1]


def process_signal(db_client, handler, machine,
                   signal, signal_type, job_name, days_back):
    """
    Function that reads data for a certain signal for a certain machine
    :param db_client: the database client that will be used
    :param handler: the handler
    :param machine: the machine
    :param signal: the signal to look for
    :param signal_type: the signal type
    :param job_name: the name of the job
    :param days_back: days_back that should be loaded as an overwrite for redis
    :return:-
    """

    full_signal = "m{0}.{1}.{2}".format(machine[MACHINE_NR], signal_type, signal[NAME])
    signal_id = "SIGNAL:" + ".".join([handler, full_signal])
    last_updated = get_last_updated(signal_id, days_back)
    while True:
        (rows, no_errors, time_spent) = db_client.get_all(
            PMA_QUERY, [signal[NAME], machine[MACHINE_NR],
                        last_updated.strftime(DATETIME_FORMAT), CHUNKSIZE])
        global total_no_errors
        total_no_errors += no_errors
        global total_time_spent
        total_time_spent += time_spent

        if len(rows) > 0:
            idx = [row[0] for row in rows]
            data = [row[1] for row in rows]
            idx = pd.to_datetime(idx).tz_localize(machine["timezone"], ambiguous="NaT")
            out = dict()
            out[signal[NAME]] = pd.Series(data=data, index=idx)
            save_to_db(
                data=out,
                prefix="s{0}.{1}".format(machine[SOURCE_NR], signal_type),
                job_name=job_name)
            last_updated = arrow.get(idx[-1])
            REDIS_CLIENT.set(signal_id, last_updated.timestamp)

        if len(rows) < CHUNKSIZE:
            break


def write_monitor_data():
    """
    Keeps track of the pma db statistics. Number of errors and total seconds spend in
    database retrieval
    :return: -
    """
    out = [{'measurement': 'pma_db_analysis',
            'fields': {'no_errors': total_no_errors, 'duration': total_time_spent},
            'time': TODAY}]

    monitor_db = sh.get_influx_client(
        user='admin', db='crawler', influxhost=sh.get_influx_monitor_host())
    monitor_db.write_points(out)


def get_last_updated(signal_id, days_back):
    """
    Returns that last updated field of the signal in either redis or as calculated per
    days_back
    :param signal_id: the id of the signal
    :param days_back: days_back that should be loaded. Used when gaps have been detected
    :return: the last updated timestamp of the signal
    """

    if days_back:
        return datetime.utcnow() - timedelta(days=days_back)
    else:
        last_update = REDIS_CLIENT.get(signal_id)  # get last update time (epoch) of this signal
        last_update = int(last_update) if last_update else arrow.now().timestamp - (
                    3600 * 24 * 7)  # First time:  one week back
        last_update = arrow.get(last_update)
        return last_update


def get_signals_info(handler):
    """
    Get information regarding all the signals registered for the handler
    :param handler: the handler
    :return: signal info
    """

    handlers = Config().get_handlers()
    for hdl in handlers:
        if hdl[NAME] == handler:
            signals = hdl["signals"]
            signal_type = hdl["signal_type"]
            return signals, signal_type
    else:
        return None, None
