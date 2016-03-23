__author__ = 'jmettu'
from datetime import datetime
from datetime import timedelta
from dateutil import tz
import json
import pytz
from pytz import timezone
import requests
import time
import luigi

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
    jobserver_context = luigi.Parameter(significant=False)
    datacube_jar = luigi.Parameter(significant=False)
    cassandra_keyspace = luigi.Parameter(significant=False)
    cassandra_namespace = luigi.Parameter(significant=False)
    appserver_host = luigi.Parameter(significant=False)
    sqlcmv_hdfsdir = luigi.Parameter(significant=False)
    sqlcmv_keyspace = luigi.Parameter(significant=False)
    sqlcmv_nameSpace = luigi.Parameter(significant=False)
    cassandra_seeds = luigi.Parameter(significant=False)
    jobserver_host_port = luigi.Parameter(significant=False)
    hdfs_sessions = luigi.Parameter(significant=False)
    wario_target_db_host = luigi.Parameter(significant=False)
    wario_target_db_user = luigi.Parameter(significant=False)
    wario_target_db_password = luigi.Parameter(significant=False)
    wario_target_db_name = luigi.Parameter(significant=False)

    def requires(self):
        pass

    def run(self):
        pass

    def output(self):
        pass

class CmvLib:

    @staticmethod
    def check_boundaries(date_time):
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
            pcode_info[pcode_tz[0]] = {'numOfPartition':1, 'input-paths':[], 'timezone':pcode_tz[1]}
        if len(pcode_info) > 0:
            pcode_info.values()[0]['input-paths'] = file_list
        return pcode_info

    @staticmethod
    def submit_config_to_js(config_json, js_url):
        headers = {'content-type': 'application/json'}
        logging.info("Submitting jobserver config to url: %s", js_url)
        r = requests.post(js_url, json.dumps(config_json), headers)
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
    def submit_config_to_appsvr(config_json, appsvr_url):
        logging.debug("Submitting appsvr config to url: %s", appsvr_url)
        r = requests.post(appsvr_url, json.dumps(config_json))
        r.raise_for_status()
        submission_status = r.json()
        logging.debug("Appsvr response: %s", submission_status)
        return submission_status

    @staticmethod
    def poll_appsvr_job_status(job_status_url):
        logging.info('Started polling Appsvr...')
        while True:
            job_status = requests.get(job_status_url).json()
            if job_status['status'] != 'RUNNING':
                return job_status
            time.sleep(30)


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
