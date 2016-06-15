"""Rollup base task"""

from datetime import date
import json
import logging
import time

from datadog import statsd
import luigi

from wario.lib.cmv_mysql_target import CmvMysqlTarget
from wario.lib.cmvlib import CmvLib
from wario.lib.cmvlib import CmvBaseTask
from wario.lib.cmvlib import DataDogClient

class CmvRollupBaseTask(CmvBaseTask):
    """Base task for rollup"""
    day = luigi.DateParameter(default=date(year=2016, month=3, day=7))
    pcode = luigi.Parameter(default='VzcGw6NlhJZUFfutRhfdpVYIQrRp')
    timezone = luigi.Parameter(default='Asia/Kolkata')
    rollup_namespace = luigi.Parameter(default='nst-rollup', significant=False)
    wario_target_table_name = luigi.Parameter(significant=False)
    appserver_app_name = luigi.Parameter(significant=False)
    appserver_app_type = luigi.Parameter(significant=False)
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

    def get_rdd_duration(self):
        """Returns the rdd duration to roll"""
        pass

    def get_rdd_rolled_duration(self):
        """Returns the rdd duration rolled to"""
        pass

    def get_appserver_job_config(self):
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
            'rdd_duration': self.get_rdd_duration(),
            'rdd_rollup_duration': self.get_rdd_rolled_duration()
        }

        with open(CmvLib.get_template_path('resources/rollup_template.json')) as tmpl_file:
            cfg = json.load(tmpl_file)
            CmvLib.replace_config_params(cfg, tmpl_values)
            return cfg

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
        job_cfg = self.get_appserver_job_config()
        logging.info('Running rollup job...')
        appserver_jobsubmit_url = CmvLib.get_appserver_job_submit_url(self.appserver_host_port,
                                                                      self.appserver_app_name,
                                                                      self.appserver_app_type)
        rslt_json = CmvLib.submit_config_to_appserver(job_cfg, appserver_jobsubmit_url)
        job_id = rslt_json['payload']['jobId']
        appserver_jobstatus_url = CmvLib.get_appserver_job_status_url(self.appserver_host_port,
                                                                      self.appserver_app_name,
                                                                      job_id)
        appserver_resp = CmvLib.poll_appserver_job_status(appserver_jobstatus_url)
        if appserver_resp['payload']['status'] != 'Finished':
            logging.error("AppServer responded with an error. AppServer Response: %s",
                          appserver_resp['payload']['result'])
            raise Exception('Error in Appserver Response.')
        else:
            logging.info("Rollup job completed successfully.")
        self.output().touch()
        statsd.histogram(self.metric_name, time.time()-datadog_start_time, tags=self.tag_name)

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
