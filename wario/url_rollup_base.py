"""Url rollup base task"""

from datetime import date
import json
import logging
import time

import luigi

from wario.lib.cmv_mysql_target import CmvMysqlTarget
from wario.lib.cmvlib import CmvLib
from wario.lib.cmvlib import CmvBaseTask

class UrlRollupBaseTask(CmvBaseTask):
    """Base task for url rollup"""
    day = luigi.DateParameter(default=date(year=2016, month=3, day=25))
    rollup_namespace = luigi.Parameter(default='nst-rollup', significant=False)
    appserver_app_name = luigi.Parameter(default='rollup', significant=False)
    wario_target_table_name = luigi.Parameter(significant=False)

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

    def get_appserver_job_config(self):
        """Returns a rollup job config"""
        datetime_fmt = "%Y-%m-%dT%H:%M"
        tmpl_values = {
            'cache_namespace': self.cassandra_namespace,
            'rollup_namespace': self.rollup_namespace,
            'cassandra_seeds': self.cassandra_seeds.split(','),
            'cassandra_keyspace': self.cassandra_keyspace,
            'start_time': self.get_start_time().strftime(datetime_fmt),
            'end_time': self.get_end_time().strftime(datetime_fmt),
            'rdd_duration': self.get_rdd_duraion(),
            'rdd_rollup_duration': self.get_rdd_rolled_duration(),
            "hdfs_cmv_dir": self.hdfs_cmv_dir
        }

        with open("wario/utils/url_rollup_template.json") as tmpl_file:
            cfg = json.load(tmpl_file)
            CmvLib.replace_config_params(cfg, tmpl_values)
            return cfg

    def run(self):
        job_cfg = self.get_appserver_job_config()
        logging.info('Running url rollup job...')
        submission_status = CmvLib.submit_config_to_appserver(
            job_cfg,
            CmvLib.get_appserver_job_submit_url(self.appserver_host_port, self.appserver_app_name)
        )
        job_id = submission_status['result']['jobId']
        time.sleep(5)
        job_status = CmvLib.poll_appserver_job_status(
            CmvLib.get_appserver_job_status_url(
                self.appserver_host_port,
                self.appserver_app_name,
                job_id
            )
        )
        if job_status['status'] != 'OK':
            logging.error("App Server responded with an error. App Server Response: %s", job_status)
            raise Exception('Error in App Server Response.')
        else:
            logging.info("Url rollup job completed successfully.")
        self.output().touch()

    def output(self):
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
            'day': self.day.strftime(datefmt)
        }
        return CmvMysqlTarget(connect_args, col_values)

if __name__ == '__main__':
    luigi.run(['UrlRollupBaseTask', '--workers', '1'])