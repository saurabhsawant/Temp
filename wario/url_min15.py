"""Url min15 task"""

import json
from datetime import datetime, timedelta
import logging
import time

import luigi

from lib.cmv_mysql_target import CmvMysqlTarget
from lib.cmvlib import CmvBaseTask
from lib.cmvlib import CmvLib
from lib.cmvlib import InputSessionFile
from lib.cmvlib import Json

class UrlMin15(CmvBaseTask):
    """Task for url min15 data generation"""
    start_time = luigi.DateMinuteParameter(
        default=datetime(year=2016, month=3, day=21, hour=12, minute=15)
    )
    wario_target_table_name = luigi.Parameter(default='url_min15', significant=False)

    def get_appsvr_job_config(self):
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
        with open("wario/utils/url_min15_template.json") as tmpl_file:
            cfg = json.load(tmpl_file)
            CmvLib.replace_config_params(cfg, tmpl_values)
            return cfg

    def get_appserver_job_status_url(self, job_id):
        """Returns job status url"""
        appserver_url = \
            'http://{appserver_host_port}/apps/{app_name}/jobs/{job_id}/status'.format(
                appserver_host_port=self.appserver_host_port,
                app_name=self.appserver_app_name,
                job_id=job_id
            )
        return appserver_url

    def get_appserver_job_submission_url(self):
        """Returns job submission url"""
        appserver_url = \
            'http://{appserver_host_port}/apps/{app_name}/jobs?timeout=100&sync=false'.format(
                appserver_host_port=self.appserver_host_port,
                app_name=self.appserver_app_name
            )
        return appserver_url

    def requires(self):
        return InputSessionFile(cube_time=self.start_time)

    def run(self):
        job_cfg = self.get_appsvr_job_config()
        logging.info('Running url min15 job...')
        submission_status = CmvLib.submit_config_to_appserver(
            job_cfg,
            self.get_appserver_job_submission_url()
        )
        job_id = submission_status['result']['jobId']
        time.sleep(5)
        job_status = CmvLib.poll_appserver_job_status(self.get_appserver_job_status_url(job_id))
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
    luigi.run(['UrlMin15', '--workers', '1'])
