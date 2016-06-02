#
# __author__ = 'baohua'
#
"""Url min15 task"""

import json
from datetime import datetime, timedelta
import logging
import time

import luigi

from wario.lib.cmv_mysql_target import CmvMysqlTarget
from wario.lib.cmvlib import CmvBaseTask
from wario.lib.cmvlib import CmvLib
from wario.lib.cmvlib import InputSessionFile
from wario.lib.cmvlib import DataDogClient

class UrlMin15Generator(CmvBaseTask):
    """Task for url min15 data generation"""
    start_time = luigi.DateMinuteParameter(
        default=datetime(year=2016, month=3, day=21, hour=12, minute=15)
    )
    wario_target_table_name = luigi.Parameter(default='url_min15', significant=False)

    def get_appserver_job_config(self):
        """Returns job config"""
        tmpl_values = {
            "start_time": CmvLib.date_to_cmvformat(self.start_time),
            "end_time": CmvLib.date_to_cmvformat(self.start_time + timedelta(minutes=15)),
            "cache_namespace": self.cassandra_namespace,
            "cassandra_seeds": self.cassandra_seeds.split(','),
            "cassandra_keyspace": self.cassandra_keyspace,
            "hdfs_name_node": self.hdfs_namenode,
            "hdfs_session_dirs": self.hdfs_session_dirs.split(','),
            "hdfs_cmv_dir": self.hdfs_cmv_dir
        }
        with open(CmvLib.get_template_path('resources/url_min15_template.json')) as tmpl_file:
            cfg = json.load(tmpl_file)
            CmvLib.replace_config_params(cfg, tmpl_values)
            return cfg

    def requires(self):
        return InputSessionFile(cube_time=self.start_time)

    def run(self):
        job_cfg = self.get_appserver_job_config()
        logging.info('Running url min15 job...')
        datadog_start_time = time.time()
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
        elapsed_time = (time.time()-datadog_start_time)/60
        DataDogClient.gauge_this_metric('url_min15_delay', elapsed_time)
        if job_status['status'] != 'OK':
            logging.error("Job Server responded with an error. Job Server Response: %s", job_status)
            raise Exception('Error in Job Server Response.')
        else:
            logging.info("Url min15 job completed successfully.")
        self.output().touch()

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
    luigi.run(['CmvUrlMin15Generator', '--workers', '1'])
