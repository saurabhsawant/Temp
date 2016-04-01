__author__ = 'jmettu'
from datetime import datetime
from datetime import timedelta
from dateutil import tz
import json
import pytz
from pytz import timezone
import requests
import time
import os
import wario
from datadog import statsd

from cmv_mysql_target import CmvMysqlTarget

import logging
logger = logging.getLogger('luigi-interface')

import luigi
from luigi.contrib.hdfs import HdfsTarget

class Helios:

    @staticmethod
    def get_providers_from_helios():
        connect_args = dict()
        connect_args['user'] = 'helios'
        connect_args['password'] = 'JHOibizmY07I7G31'
        connect_args['host'] = "slavedb-lb01.services.ooyala.net"
        connect_args['database'] = 'vstreams'
        connect_args['table'] = None

        query_string = """
            select P.pcode as PCODE, T.iana_name as TIMEZONE
            from providers as P, timezones as T
            where P.timezone_id=T.id and P.status != %s and status != %s and status != %s
            """
        query_values = ['churned', 'deleted', 'disabled']
        return CmvMysqlTarget(connect_args=connect_args).query(query_string, query_values)

class CmvBaseTask(luigi.Task):
    import ConfigParser
    config = ConfigParser.ConfigParser(allow_no_value=True)
    config.readfp(open(r'/etc/luigi/WarioCmv.cfg'))

    jobserver_host_port = config.get('env', 'jobserver_host_port')
    jobserver_context = config.get('env', 'jobserver_context')
    jobserver_datacube_jar = config.get('env', 'jobserver_datacube_jar')
    appserver_host_port = config.get('env', 'appserver_host_port')
    appserver_app_name = config.get('env', 'appserver_app_name')

    cassandra_keyspace = config.get('cassandra', 'cassandra_keyspace')
    cassandra_namespace = config.get('cassandra', 'cassandra_namespace')
    cassandra_seeds = config.get('cassandra', 'cassandra_seeds')

    hdfs_namenode = config.get('hadoop', 'hdfs_namenode')
    hdfs_session_dirs = config.get('hadoop', 'hdfs_session_dirs')
    hdfs_cmv_dir = config.get('hadoop', 'hdfs_cmv_dir')

    wario_target_db_host = config.get('wario_db', 'wario_target_db_host')
    wario_target_db_user = config.get('wario_db', 'wario_target_db_user')
    wario_target_db_password = config.get('wario_db', 'wario_target_db_password')
    wario_target_db_name = config.get('wario_db', 'wario_target_db_name')

    def _type(self):
        return self.__class__.__name__

    def requires(self):
        pass

    def run(self):
        pass

    def output(self):
        pass

class CmvLib:

    @staticmethod
    def get_template_path(file_name):
        return os.path.join(wario.__path__[0], file_name)

    @staticmethod
    def validate_min15_time(date_time):
        if date_time.minute % 15 != 0:
            raise ValueError('Given time %s is not at 15 min boundary' % date_time)

    @staticmethod
    def replace_config_params(json_data, tmpl_subst_params):
        for k in json_data:
            key_type = type(json_data[k])
            if key_type is dict:
                CmvLib.replace_config_params(json_data[k], tmpl_subst_params)
            elif key_type is unicode and json_data[k].startswith('$'):
                json_data[k] = tmpl_subst_params[json_data[k][1:]]

    @staticmethod
    def prepare_ptz(pcode_tz_rows, file_list):
        pcode_info = {}
        for pcode_tz in pcode_tz_rows:
            pcode_info[pcode_tz[0]] = {'numOfPartition': 1, 'input-paths': [], 'timezone': pcode_tz[1]}
        if len(pcode_info) > 0:
            pcode_info.values()[0]['input-paths'] = file_list
        return pcode_info

    @staticmethod
    def submit_config_to_js(config_json, js_url):
        headers = {'content-type': 'application/json'}
        logging.info("Submitting jobserver config to url: %s", js_url)
        r = requests.post(js_url, json.dumps(config_json), headers)
        DataDogClient.gauge_http_status('job_server', r.status_code)
        r.raise_for_status()
        js_rslt = r.json()
        logging.info("Job Server response: %s", js_rslt)
        return js_rslt

    @staticmethod
    def date_to_cmvformat(dt):
        return '{y}-{mo}-{d}T{h}:{mi}Z'.format(y=dt.year, mo=dt.month, d=dt.day, h=dt.hour, mi=int(dt.minute/15)*15)

    @staticmethod
    def poll_js_jobid(job_id, js_host):
        logging.info('Started polling Job Server')
        while True:
            js_resp = requests.get('http://{js_host}/jobs/{job_id}'
                                   .format(js_host=js_host, job_id=job_id)).json()
            DataDogClient.gauge_http_status('job_server', js_resp.status_code)
            if js_resp['status'] != 'RUNNING':
                return js_resp
            time.sleep(120)

    @staticmethod
    def poll_js_jobid_urllib(job_id, js_host):
        import urllib2
        logging.info('Started polling Job Server')
        while True:
            resp = urllib2.urlopen('http://{js_host}/jobs/{job_id}'
                                   .format(js_host=js_host, job_id=job_id))
            json_resp = json.load(resp)
            if json_resp['status'] != 'RUNNING':
                return json_resp
            time.sleep(120)

    @staticmethod
    def poll_js_jobid_curl(job_id, js_host):
        import subprocess
        import re
        logging.info('Started polling Job Server')
        js_url = 'http://{js_host}/jobs/{job_id}'.format(js_host=js_host, job_id=job_id)
        while True:
            commandJob = 'curl {js_url}'.format(js_url=js_url)
            sleepTime = 5
            sleepWork = 10
            for num in range(10000):
                print("Sleeping ", num)
                time.sleep(sleepTime)
                sleepTime = sleepWork
                p = subprocess.Popen(commandJob, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                (output, err) = p.communicate()
                matchStr = '.*"status": "([^"]*)"'
                jobResObj = re.match(matchStr, re.sub("\n", " ", output))
                if jobResObj and (not re.search("RUNNING", jobResObj.group(1))):
                    logging.info("command: " + commandJob + "\nresult " + output[-100:] + " err? " + err + "\n")
                    logging.info("curl {num}".format(num=num))
                    return CmvLib.poll_js_jobid_requests(job_id, js_host)
            return CmvLib.poll_js_jobid_requests(job_id, js_host)

    @staticmethod
    def submit_config_to_appserver(config_json, appserver_url):
        logging.info("Submitting app server config to url: %s", appserver_url)
        r = requests.post(appserver_url, json.dumps(config_json))
        DataDogClient.gauge_http_status('app_server', r.status_code)
        r.raise_for_status()
        submission_status = r.json()
        logging.info("Appsvr response: %s", submission_status)
        return submission_status

    @staticmethod
    def poll_appserver_job_status(job_status_url):
        logging.info('Started polling app server...')
        while True:
            job_status = requests.get(job_status_url).json()
            DataDogClient.gauge_http_status('app_server', job_status.status_code)
            if job_status['status'] != 'RUNNING':
                return job_status
            time.sleep(30)

    @staticmethod
    def get_appserver_job_submit_url(appserver_host_port, app_name):
        appserver_url = \
            'http://{appserver_host_port}/apps/{app_name}/jobs?timeout=100&sync=false'.format(
                appserver_host_port=appserver_host_port,
                app_name=app_name
            )
        return appserver_url

    @staticmethod
    def get_appserver_job_status_url(appserver_host_port, app_name, job_id):
        appserver_url = \
            'http://{appserver_host_port}/apps/{app_name}/jobs/{job_id}/status'.format(
                appserver_host_port=appserver_host_port,
                app_name=app_name,
                job_id=job_id
            )
        return appserver_url

class DataDogClient:

    @staticmethod
    def gauge_this_metric(metric_name, metric_val, tags=None):
        statsd.gauge('wario.datacompute.'+metric_name, metric_val, tags)

    @staticmethod
    def gauge_http_status(metric_name, req_status_code, tags=None):
        if req_status_code == 200:
            DataDogClient.gauge_this_metric(metric_name+'.200', 1, tags)
        elif 400 <= req_status_code < 500:
            DataDogClient.gauge_this_metric(metric_name+'.400', 1, tags)
        elif 500 <= req_status_code < 600:
            DataDogClient.gauge_this_metric(metric_name+'.500', 1, tags)


class InputSessionFile(luigi.ExternalTask):
    cube_time = luigi.DateMinuteParameter()
    hdfs_sessions = luigi.Parameter(significant=False)
    hdfs_namenode = luigi.Parameter(significant=False)

    def output(self):
        hdfs_str = self.cube_time.strftime(self.hdfs_sessions+'/%Y/%m/%d/%H/%M/_SUCCESS')
        logging.info(hdfs_str)
        return HdfsTarget(hdfs_str)


class Json:

    @staticmethod
    def pretty_dumps(json_data):
        return json.dumps(json_data, indent=4, separators=(',', ': '))


class DateTime:

    @staticmethod
    def validate_weekday(day):
        if day.weekday() != 0:
            raise ValueError('Given day, %s, does not start on Monday' % day)

    @staticmethod
    def validate_start_of_month(day):
        if day.day != 1:
            raise ValueError('Given day, %s, does not start of the month' % day)

    @staticmethod
    def get_last_day_of_month(date):
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - timedelta(days=1)

    @staticmethod
    def utc_to_any_tz(utc_datetime, tz_iana_name):
        """Converts utc time to a different timezone based on the given iana name
            :param utc_datetime: utc datetime object
            :param tz_iana_name : tz iana name
            :returns new timezone datetime object
        """
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz(tz_iana_name)
        utc = utc_datetime.replace(tzinfo=from_zone)
        return utc.astimezone(to_zone)

    @staticmethod
    def date_to_utc(date, timezone_str):
        """Converts date in timezone to utc"""
        tz = timezone(timezone_str)
        day = datetime(date.year, date.month, date.day)
        return tz.localize(day).astimezone(pytz.utc)

    @staticmethod
    def next_rounded_min15(dateminute):
        """Return the next rounded min15"""
        round_minutes = 15
        min15 = dateminute.replace(minute=(dateminute.minute / round_minutes) * round_minutes)
        return min15 + timedelta(minutes=round_minutes)

    @staticmethod
    def next_day(date):
        """Return one day after date"""
        return datetime(date.year, date.month, date.day) + timedelta(days=1)


class day_utc_min15_iter:
    """Iterator over the list of utc min15s covered by the given day in the time zone"""
    def __init__(self, day, timezone_str):
        self.min15 = DateTime.date_to_utc(day, timezone_str)
        self.last_min15 = DateTime.date_to_utc(day + timedelta(days=1), timezone_str)

    def __iter__(self):
        return self

    def next(self):
        if self.min15 != self.last_min15:
            min15 = self.min15
            self.min15 = DateTime.next_rounded_min15(min15)
            return min15
        else:
            raise StopIteration()
