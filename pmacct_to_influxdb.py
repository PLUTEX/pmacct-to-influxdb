from influxdb import InfluxDBClient, exceptions
from datetime import datetime, timedelta
from threading import Timer, Thread, main_thread
from sys import exit
from json import loads
from json.decoder import JSONDecodeError
from inotify.adapters import Inotify
from _thread import interrupt_main
import socket
import logging


# Logging options
logging.basicConfig(filename='pmacct_to_influxdb.log',
                    format='%(asctime)s (%(levelname)s) %(message)s')
logger = logging.getLogger('pmacct_to_influxdb')
logger.setLevel(logging.ERROR)


# InfluxDB connection data
_INFLUX_HOST = 'x.x.x.x'
_INFLUX_PORT = 8086
_INFLUX_USERNAME = '_username_'
_INFLUX_PASSWORD = '_password_'
_INFLUX_DATABASE = 'sflow_global_traffic'


# InfluxDB measurements for pmacct data
_TRAFFIC_MEASUREMENT = 'traffic'
_TOP_TALKERS_MEASUREMENT = 'top_talkers'
_TOP_TALKERS_COUNT = 30


# Pmacct data file
_PMACCT_DATA = 'global_asn_traffic.txt'


# ASN to names data file
_ASN_TO_NAMES = 'asn_to_names.txt'

ASN_TO_NAMES = None


def load_asn_to_names():
    """ Return ASN to AS name and AS country mapping
    On first call, this mapping is loaded from _ASN_TO_NAMES into RAM.
    """
    global ASN_TO_NAMES
    if ASN_TO_NAMES is None:
        with open(_ASN_TO_NAMES) as asn_file:
            ASN_TO_NAMES = {
                int(x[0]): (x[1], x[2])
                for x in (line.rstrip('\n').split('\t') for line in asn_file)
            }
    return ASN_TO_NAMES


def format_asn(asn):
    """ Nicely format AS number, name and country """
    asn_list = load_asn_to_names().get(asn) or ('XX', 'UNKNOWN')
    return 'AS{} {} ({})'.format(
        asn,
        asn_list[1],
        asn_list[0],
    )


def write_to_db():
    """ When called function reads pmacct JSONized data stored in _PMACCT_DATA file:
        {"packets": 36542464, "as_dst": 123, "bytes": 52984530944}
        {"packets": 389120, "as_dst": 124, "bytes": 572760064}
        {"packets": 4237312, "as_dst": 125, "bytes": 6393528320}
        ...
        and for each line it writes points to influxdb. Each point has 'ASN' tag with ASN description for example,
        for ASN 2856 "AS2856 BT-UK-AS (GB)"
        If any client or server side error occurs proper log entry is created and script exits all threads.
    """
    db_client = InfluxDBClient(_INFLUX_HOST, _INFLUX_PORT, _INFLUX_USERNAME, _INFLUX_PASSWORD, _INFLUX_DATABASE)
    with open(_PMACCT_DATA) as data_file:
        try:
            db_client.write_points([
                    {"measurement": _TRAFFIC_MEASUREMENT,
                     "tags": {'ASN': format_asn(line_dict['as_dst'])},
                     "fields": {'bytes': line_dict['bytes'], 'packets': line_dict['packets']}}
                    for line_dict in (loads(line) for line in data_file)],
                    batch_size=1000)
        except (exceptions.InfluxDBClientError, exceptions.InfluxDBServerError) as db_error:
            logger.error('Could not write point to DB: {}'.format(str(db_error)[:-1]))
            logger.error('Exiting...')
            # Canceling Timer thread
            timer_obj.cancel()
            # Sending KeyboardInterrupt to main thread
            interrupt_main()
        except JSONDecodeError as json_error:
            logger.error('Could not decode JSON, ignoring this update: {}'.format(str(json_error)))


def watch_prefix_file(file_name):
    """ Using inotify function is looking for IN_CLOSE_WRITE events, that happens when pmacct is pushing new data to
        _PMACCT_DATA file. write_to_db is called to store new data into database. On every iteration main thread
        status is checked.
    """
    inotify_obj = Inotify()
    inotify_obj.add_watch(file_name)
    try:
        for event in inotify_obj.event_gen():
            if event is not None:
                if event[1] == ['IN_CLOSE_WRITE']:
                    logger.debug("Found IN_CLOSE_WRITE event")
                    write_to_db()
            else:
                if not main_thread().is_alive():
                    logger.error('Main thread died, stopping all child threads')
                    # Canceling Timer thread
                    timer_obj.cancel()
                    # Breaking watcher thread loop
                    break
    finally:
        inotify_obj.remove_watch(file_name)


def clear_top_talkers_measurement():
    """ On every call ASNs that had highest max bps traffic over last day are chosen and written in
    _TOP_TALKERS_MEASUREMENT, that allows templating in grafana. _TOP_TALKERS_COUNT specifies how many ASNs are stored.
    """
    db_client = InfluxDBClient(_INFLUX_HOST, _INFLUX_PORT, _INFLUX_USERNAME, _INFLUX_PASSWORD, _INFLUX_DATABASE)
    try:
        db_client.delete_series(_INFLUX_DATABASE, _TOP_TALKERS_MEASUREMENT)
        db_client.query('SELECT TOP(bytes,ASN,{}) INTO {} FROM {} WHERE time > now() - 1d'
                        ''.format(_TOP_TALKERS_COUNT, _TOP_TALKERS_MEASUREMENT, _TRAFFIC_MEASUREMENT),
                        database=_INFLUX_DATABASE)
    except (exceptions.InfluxDBClientError, exceptions.InfluxDBServerError) as db_error:
        logger.error('Could not update : {}'.format(db_error))
        logger.error('Exiting...')
        # Canceling Timer thread
        timer_obj.cancel()
        # Sending KeyboardInterrupt to main thread
        interrupt_main()


if __name__ == '__main__':
    load_asn_to_names()
    try:
        # Starting inotify watcher thread
        watcher = Thread(target=watch_prefix_file, name='prefix_file_watcher', args=(bytes(_PMACCT_DATA, 'utf-8'),))
        watcher.start()
        while main_thread().is_alive():
            current_time = datetime.today()
            # Setting timer for clear_top_talkers_measurement function to 3am each day
            replaced_time = current_time.replace(hour=3, minute=0, second=0, microsecond=0)
            run_time = replaced_time + timedelta(1)
            timer_obj = Timer((run_time - current_time).total_seconds(), clear_top_talkers_measurement)
            timer_obj.name = 'clear_timer'
            timer_obj.start()
            timer_obj.join()
            logger.debug("Cleared top talkers at {}".format(run_time))
    except KeyboardInterrupt:
        logger.error("Main thread received KeyboardInterrupt signal and closed")
        exit(1)
