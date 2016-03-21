import json
from datetime import timedelta
import logging
import time

from lib.cmvlib import *
import luigi

class UrlMin15(luigi.Task):
    start_time = luigi.DateMinuteParameter(default=datetime(year=2016, month=3, day=8, hour=12, minute=15))
    cache_namespace = luigi.Parameter(default='nst_namespace_url', significant=False)
    cassandra_keyspace = luigi.Parameter(default='nst_keyspace_url', significant=False)
    cassandra_seeds = luigi.Parameter(
        default='cass-next-staging1.services.ooyala.net,' \
                'cass-next-staging2.services.ooyala.net,' \
                'cass-next-staging3.services.ooyala.net',
        significant=False
    )
    hdfs_namenode = luigi.Parameter(default='hdfs://cdh5qa', significant=False)
    hdfs_session_dirs = luigi.Parameter(default='/delphi/next-staging/sessions', significant=False)
    hdfs_cmv_dir = luigi.Parameter(default='/delphi/next-staging/cmv', significant=False)
    appsvr_host_port = luigi.Parameter(
        default='jobserver-next-staging3.services.ooyala.net:9000',
        significant=False
    )
    appsvr_app_name = luigi.Parameter(default='cmvsql', significant=False)
    urlmin15_target_db_name = luigi.Parameter(default='wario', significant=False)
    urlmin15_target_table_name = luigi.Parameter(default='url_min15', significant=False)

    def get_appsvr_job_config(self):
        tmpl_values = {
            "start_time": CmvLib.date_to_cmvformat(self.start_time),
            "end_time": CmvLib.date_to_cmvformat(self.start_time + timedelta(minutes=15)),
            "cache_namespace": self.cache_namespace,
            "cassandra_seeds": self.cassandra_seeds.split(','),
            "cassandra_keyspace": self.cassandra_keyspace,
            "hdfs_name_node": self.hdfs_namenode,
            "hdfs_session_dirs": self.hdfs_session_dirs.split(','),
            "hdfs_cmv_dir": self.hdfs_cmv_dir
        }
        with open("wario/utils/urlmin15_template.json") as tmpl_file:
            cfg = json.load(tmpl_file)
            CmvLib.replace_config_params(cfg, tmpl_values)
            return cfg

    def get_appsvr_job_status_url(self, job_id):
        appsvr_url = \
            'http://{appsvr_host_port}/apps/{app_name}/jobs/{job_id}'.format(
                appsvr_host_port=self.appsvr_host_port,
                app_name=self.appsvr_app_name,
                job_id=job_id
            )
        return appsvr_url

    def get_appsvr_job_submission_url(self):
        """Returns the job submission url"""
        appsvr_url = \
            'http://{appsvr_host_port}/apps/{app_name}/jobs?timeout=100&sync=false'.format(
                appsvr_host_port=self.appsvr_host_port,
                app_name=self.appsvr_app_name
            )
        return appsvr_url

    def requires(self):
        return InputSessionFile(cube_time=self.start_time)

    def run(self):
        job_cfg = self.get_appsvr_job_config()
        logging.info('Running url min15 job...')
        submit_status = CmvLib.submit_config_to_appsvr(job_cfg, self.get_appsvr_job_submission_url())
        job_id = submit_status['result']['jobId']
        time.sleep(5)
        job_status = CmvLib.poll_appsvr_job_status(self.get_appsvr_job_status_url(job_id))
        if job_status['status'] != 'OK':
            logging.error("Job Server responded with an error. Job Server Response: %s", job_status)
            raise Exception('Error in Job Server Response.')
        else:
            logging.info("Url min15 job completed successfully.")
        self.output().touch()

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
    luigi.run(['UrlMin15', '--workers', '1'])
