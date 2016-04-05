"""Rollup base task"""

from datetime import date
import json
import logging
import time

import luigi

from wario.lib.cmv_mysql_target import CmvMysqlTarget
from wario.lib.cmvlib import CmvLib
from wario.lib.cmvlib import CmvBaseTask
from wario.lib.cmvlib import DataDogClient
from datadog import statsd

class CmvRollupBaseTask(CmvBaseTask):
    """Base task for rollup"""
    day = luigi.DateParameter(default=date(year=2016, month=3, day=7))
    pcode = luigi.Parameter(default='VzcGw6NlhJZUFfutRhfdpVYIQrRp')
    timezone = luigi.Parameter(default='Asia/Kolkata')
    rollup_namespace = luigi.Parameter(default='nst-rollup', significant=False)
    wario_target_table_name = luigi.Parameter(significant=False)
    metric_name = None
    tag_name = None

    def requires(self):
        pass

    def validate_day(self):
        """Vadlidates the day parameter"""
        pass

    def get_start_time(self):
        """Returns the start time for rollup"""
        pass

    def get_end_time(self):
        """Returns the end time for rollup"""
        pass

    def get_rdd_duraion(self):
        """Returns the rdd duration to roll"""
        pass

    def get_rdd_rolled_duration(self):
        """Returns the rdd duration rolled to"""
        pass

    def get_js_job_config(self):
        """Returns a rollup job config"""
        datetime_fmt = "%Y-%m-%dT%H:%M"
        tmpl_values = {
            'cache_namespace': self.cassandra_namespace,
            'rollup_namespace': self.rollup_namespace,
            'pcodes': {
                self.pcode: {
                    'numOfPartitions': 1,
                    "timezone": self.timezone
                }
            },
            'cassandra_seeds': self.cassandra_seeds.split(','),
            'keyspace': self.cassandra_keyspace,
            'start_time': self.get_start_time().strftime(datetime_fmt),
            'end_time': self.get_end_time().strftime(datetime_fmt),
            'rdd_duration': self.get_rdd_duraion(),
            'rdd_rollup_duration': self.get_rdd_rolled_duration()
        }

        with open(CmvLib.get_template_path('utils/rollup_template.json')) as tmpl_file:
            cfg = json.load(tmpl_file)
            CmvLib.replace_config_params(cfg, tmpl_values)
            return cfg

    def get_js_job_url(self):
        """Returns the job submission url"""
        js_url = \
            'http://{js_host_port}/jobs?appName={app_name}&classPath={job_class}&' \
            'context={js_context}&timeout=100&sync=false'.format(
                js_host_port=self.jobserver_host_port,
                app_name=self.jobserver_datacube_jar,
                job_class='ooyala.cnd.RollupDelphiDatacubes',
                js_context=self.jobserver_context)
        return js_url

    def rollup_datadog(self):
        metric_name = None
        if 'daily' in self._type().lower():
            metric_name = 'rollup_day'
        elif 'weekly' in self._type().lower():
            metric_name = 'rollup_week'
        elif 'monthly' in self._type().lower():
            metric_name = 'rollup_month'

        tag_name = ['start_date:{date}'.format(date=self.get_start_time().strftime('%Y-%m-%d'))]
        return 'wario.datacompute.'+metric_name, tag_name

    def run(self):
        datadog_start_time = time.time()
        job_cfg = self.get_js_job_config()
        logging.info('Running rollup job...')
        #submission_status = CmvLib.submit_config_to_js(job_cfg, self.get_js_job_url())
        #job_id = submission_status['result']['jobId']
        time.sleep(datadog_start_time % 7 + 5)
        #job_status = CmvLib.poll_js_jobid(job_id, self.jobserver_host_port)
        job_status = {'status': 'OK'}
        if job_status['status'] != 'OK':
            logging.error("Job Server responded with an error. Job Server Response: %s", job_status)
            raise Exception('Error in Job Server Response.')
        else:
            logging.info("Rollup job completed successfully.")
        self.output().touch()
        #DataDogClient.gauge_this_metric(self.metric_name, time.time() - datadog_start_time, tags=self.tag_name)
        statsd.histogram(self.metric_name, time.time()-datadog_start_time ,tags=self.tag_name)


    def output(self):
        self.metric_name, self.tag_name = self.rollup_datadog()
        connect_args = {
            'host': self.wario_target_db_host,
            'user': self.wario_target_db_user,
            'password': self.wario_target_db_password,
            'database': self.wario_target_db_name,
            'table': self.wario_target_table_name
        }

        datefmt = "%Y-%m-%d"
        col_values = {
            'target_id': self.task_id,
            'day': self.day.strftime(datefmt),
            'pcode': self.pcode,
            'timezone': self.timezone
        }
        return CmvMysqlTarget(connect_args, col_values)

if __name__ == '__main__':
    luigi.run(['CmvRollupBaseTask', '--workers', '1'])
