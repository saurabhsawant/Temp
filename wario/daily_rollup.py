from datetime import datetime
import json
import logging
import luigi
import time

from cmv import Cmv
from lib.cmvlib import CmvLib
from lib.cmv_utils import pretty_json
from lib.datetime_lib import date_to_utc, next_rounded_min15, next_day
from lib.mysql_lib import create_mysql_target

class DailyRollup(luigi.Task):
    day = luigi.DateParameter(default=datetime(year=2016,month=3,day=8))
    pcode = luigi.Parameter(default='VzcGw6NlhJZUFfutRhfdpVYIQrRp')
    timezone = luigi.Parameter(default='Asia/Kolkata', significant=False)
    cache_namespace = luigi.Parameter(default='nst_namespace', significant=False)
    cassandra_keyspace = luigi.Parameter(default='nst_keyspace', significant=False)
    cassandra_seeds = luigi.Parameter(default='cass-next-staging1.services.ooyala.net,' \
                                              'cass-next-staging2.services.ooyala.net,' \
                                              'cass-next-staging3.services.ooyala.net', significant=False)
    rollup_namespace = luigi.Parameter(default='nst-rollup', significant=False)
    jobserver_host = luigi.Parameter(default='jobserver-next-staging1.services.ooyala.net:8090', significant=False)
    jobserver_app_name = luigi.Parameter(default='datacubeMaster', significant=False)
    jobserver_context = luigi.Parameter(default='next-staging', significant=False)

    def requires(self):
        if not hasattr(self, 'cmvdeps'):
            cmvdeps = []
            dateminute = date_to_utc(self.day, self.timezone)
            for i in range(0, 96):
                cmvdeps.append(Cmv(dateminute=dateminute))
                dateminute = next_rounded_min15(dateminute)
            self.cmvdeps = cmvdeps
        return self.cmvdeps

    def get_jobserver_job_config(self):
        datefmt = "%Y-%m-%dT%H:%M"
        tmpl_values = {
            'cache_namespace': self.cache_namespace,
            'rollup_namespace': self.rollup_namespace,
            'pcodes': {
                self.pcode: {
                    'numOfPartitions': 1,
                    "timezone": self.timezone
                }
            },
            'cassandra_seeds': self.parse_cassandra_seeds(self.cassandra_seeds),
            'keyspace': self.cassandra_keyspace,
            'start_time': self.day.strftime(datefmt),
            'end_time': next_day(self.day).strftime(datefmt),
            'rdd_duration': 'min15',
            'rdd_rollup_duration': 'day'
        }

        with open("utils/daily_rollup_template.json") as f:
            cfg = json.load(f)
            CmvLib.replace_config_params(cfg, tmpl_values)
            return cfg

    def parse_cassandra_seeds(self, seeds):
        return seeds.split(",")

    def get_js_url(self):
        js_url = 'http://{js_host}/jobs?appName={app_name}&classPath={job_class}&' \
                 'context={js_context}&timeout=100&sync=false'.format(
                    js_host=self.jobserver_host, app_name=self.jobserver_app_name,
                    job_class='ooyala.cnd.RollupDelphiDatacubes', js_context=self.jobserver_context)
        return js_url

    def run(self):
        cfg = self.get_jobserver_job_config()
        print(pretty_json(cfg))
        resp = CmvLib.submit_config_to_js(cfg, self.get_js_url())
        job_id = resp['result']['jobId']

        time.sleep(5)
        job_status = CmvLib.poll_js_jobid(job_id, self.jobserver_host)
        if job_status['status'] != 'OK':
            logging.info("Job Server responded with an error. Job Server Response: %s", job_status)
            raise Exception('Error in Job Server Response.')
        else:
            print(pretty_json(job_status))
        self.output().touch()

    def output(self):
        datefmt = "%Y-%m-%d"
        day_str = self.day.strftime(datefmt)
        update_id = self.pcode + ':day:' + day_str
        return create_mysql_target(update_id, day_str)
