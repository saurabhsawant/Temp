"""Daily rollup task"""

from datetime import datetime
from datetime import timedelta
import json
import logging
import time

import luigi

from wario.lib.cmv_mysql_target import CmvMysqlTarget
from wario.lib.cmvlib import CmvLib
from wario.lib.cmvlib import DateTime
from wario.lib.cmvlib import CmvBaseTask
from wario.lib.cmvlib import day_utc_min15_iter
from wario.cmvmin15 import BuildMin15Datacube

def parse_cassandra_seeds(seeds):
    """Parses a list of cassandra seeds from the given string"""
    return seeds.split(",")

class DailyRollup(CmvBaseTask):
    """Task for daily rollup"""
    day = luigi.DateParameter(default=datetime(year=2016, month=3, day=8))
    pcode = luigi.Parameter(default='VzcGw6NlhJZUFfutRhfdpVYIQrRp')
    timezone = luigi.Parameter(default='Asia/Kolkata')
    rollup_namespace = luigi.Parameter(default='nst-rollup', significant=False)
    wario_target_table_name = luigi.Parameter(default='rollup', significant=False)

    def requires(self):
        cmvmin15s = []
        for min15 in day_utc_min15_iter(self.day, self.timezone):
            cmvmin15s.append(BuildMin15Datacube(start_time=min15, end_time=min15+timedelta(minutes=15)))
        return cmvmin15s

    def get_js_job_config(self):
        """Returns a rollup job config"""
        datefmt = "%Y-%m-%dT%H:%M"
        tmpl_values = {
            'cache_namespace': self.cassandra_namespace,
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
                app_name=self.datacube_jar,
                job_class='ooyala.cnd.RollupDelphiDatacubes',
                js_context=self.jobserver_context)
        return js_url

    def run(self):
        job_cfg = self.get_js_job_config()
        logging.info('Running daily rollup job...')
        submit_status = CmvLib.submit_config_to_js(job_cfg, self.get_js_job_url())
        job_id = submit_status['result']['jobId']
        time.sleep(5)
        job_status = CmvLib.poll_js_jobid(job_id, self.jobserver_host_port)
        if job_status['status'] != 'OK':
            logging.error("Job Server responded with an error. Job Server Response: %s", job_status)
            raise Exception('Error in Job Server Response.')
        else:
            logging.info("Daily rollup job completed successfully.")
        self.output().touch()
        print ('rollup done')

    def output(self):
        connect_args = {
            'host': self.wario_target_db_host,
            'user': self.wario_target_db_user,
            'password': self.wario_target_db_password,
            'database': self.wario_target_db_name,
            'table': self.wario_target_table_name
        }
        col_values = {
            'target_id': self.task_id
        }
        return CmvMysqlTarget(connect_args, col_values)

if __name__ == '__main__':
    luigi.run(['DailyRollup', '--workers', '1'])
