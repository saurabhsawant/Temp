"""Daily rollup task"""

from datetime import datetime
import json
import logging
import time

import luigi

#from wario.cmv import Cmv
from lib.cmv_mysql_target import CmvMysqlTarget
from lib.cmvlib import CmvLib
from lib.cmvlib import DateTime
from lib.cmvlib import Json
from lib.cmvlib import day_utc_min15_iter
from datetime import timedelta
from cmvmin15 import BuildMin15Datacube

LOGGER = logging.getLogger('DailyRollup')

def parse_cassandra_seeds(seeds):
    """Parses a list of cassandra seeds from the given string"""
    return seeds.split(",")

class DailyRollup(luigi.Task):
    """Task for daily rollup"""
    day = luigi.DateParameter(default=datetime(year=2016, month=3, day=8))
    pcode = luigi.Parameter(default='VzcGw6NlhJZUFfutRhfdpVYIQrRp')
    timezone = luigi.Parameter(default='Asia/Kolkata')

    cache_namespace = luigi.Parameter(default='nst_namespace', significant=False)
    cassandra_keyspace = luigi.Parameter(default='nst_keyspace', significant=False)
    cassandra_seeds = luigi.Parameter(
        default='cass-next-staging1.services.ooyala.net,' \
                'cass-next-staging2.services.ooyala.net,' \
                'cass-next-staging3.services.ooyala.net',
        significant=False
    )
    rollup_namespace = luigi.Parameter(default='nst-rollup', significant=False)

    jobserver_host_port = luigi.Parameter(
        default='jobserver-next-staging3.services.ooyala.net:8090',
        significant=False
    )
    jobserver_app_name = luigi.Parameter(default='datacubeMaster', significant=False)
    jobserver_context = luigi.Parameter(default='next-staging', significant=False)

    rollup_target_db_host = luigi.Parameter(default='localhost', significant=False)
    rollup_target_db_user = luigi.Parameter(default='root', significant=False)
    rollup_target_db_password = luigi.Parameter(default='', significant=False)
    rollup_target_db_name = luigi.Parameter(default='cmvworkflow', significant=False)
    rollup_target_table_name = luigi.Parameter(default='rollup', significant=False)

    def requires(self):
        cmvmin15s = []
        for min15 in day_utc_min15_iter(self.day, self.timezone):
            cmvmin15s.append(BuildMin15Datacube(start_time=min15, end_time=min15+timedelta(minutes=15)))
        return cmvmin15s

    def get_js_job_config(self):
        """Returns a rollup job config"""
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
            'cassandra_seeds': parse_cassandra_seeds(self.cassandra_seeds),
            'keyspace': self.cassandra_keyspace,
            'start_time': self.day.strftime(datefmt),
            'end_time': DateTime.next_day(self.day).strftime(datefmt),
            'rdd_duration': 'min15',
            'rdd_rollup_duration': 'day'
        }

        with open("wario/utils/daily_rollup_template.json") as tmpl_file:
            cfg = json.load(tmpl_file)
            CmvLib.replace_config_params(cfg, tmpl_values)
            return cfg

    def get_js_job_url(self):
        """Returns the job submission url"""
        js_url = \
            'http://{js_host_port}/jobs?appName={app_name}&classPath={job_class}&' \
            'context={js_context}&timeout=100&sync=false'.format(
                js_host_port=self.jobserver_host_port,
                app_name=self.jobserver_app_name,
                job_class='ooyala.cnd.RollupDelphiDatacubes',
                js_context=self.jobserver_context)
        return js_url

    def run(self):
        job_cfg = self.get_js_job_config()
        submit_status = CmvLib.submit_config_to_js(job_cfg, self.get_js_job_url())
        job_id = submit_status['result']['jobId']
        time.sleep(5)
        job_status = CmvLib.poll_js_jobid(job_id, self.jobserver_host_port)
        if job_status['status'] != 'OK':
            LOGGER.error("Job Server responded with an error. Job Server Response: %s", job_status)
            raise Exception('Error in Job Server Response.')
        else:
            LOGGER.info("Job completed:\n%s", Json.pretty_dumps(job_status))
        self.output().touch()
        print ('rollup done')

    def output(self):
        connect_args = {
            'host': self.rollup_target_db_host,
            'user': self.rollup_target_db_user,
            'password': self.rollup_target_db_password,
            'database': self.rollup_target_db_name,
            'table': self.rollup_target_table_name
        }
        col_values = {
            'target_id': self.task_id
        }
        return CmvMysqlTarget(connect_args, col_values)

if __name__ == '__main__':
    luigi.run(['DailyRollup', '--workers', '1'])
