__author__ = 'jmettu'

from luigi.contrib.hdfs import HdfsTarget
from lib.cmvlib import *
import json
from datetime import timedelta
import luigi
import logging
from pprint import pprint
import time

class InputSessionFile(luigi.ExternalTask):
    cube_time = luigi.DateMinuteParameter()
    hdfs_sessions = luigi.Parameter(significant=False)
    hdfs_namenode = luigi.Parameter(significant=False)

    def output(self):
        hdfs_str = self.cube_time.strftime(self.hdfs_sessions+'/%Y/%m/%d/%H/%M/_SUCCESS')
        logging.info(hdfs_str)
        return HdfsTarget(hdfs_str)

class BuildMin15Datacube(luigi.Task):
    start_time = luigi.DateMinuteParameter()
    end_time = luigi.DateMinuteParameter()
    context = luigi.Parameter(significant=False)
    datacube_jar = luigi.Parameter(significant=False)
    key_space = luigi.Parameter(significant=False)
    name_space = luigi.Parameter(significant=False)
    appserver_host = luigi.Parameter(significant=False)
    sqlcmv_hdfsdir = luigi.Parameter(significant=False)
    sqlcmv_keyspace = luigi.Parameter(significant=False)
    sqlcmv_nameSpace = luigi.Parameter(significant=False)
    cassandra_seeds = luigi.Parameter(significant=False)
    jobserver_host = luigi.Parameter(significant=False)
    hdfs_sessions = luigi.Parameter(significant=False)
    min15_target_table_name = luigi.Parameter(significant=False)
    min15_target_db_name = luigi.Parameter(significant=False)
    hdfs_dir_set = set()
    provider_list_str = None
    connect_args = dict()
    pcode_tz_dict = dict()

    row_col_dict = dict()
    row_col_dict['ptz_dict'] = None
    row_col_dict['target_id'] = None

    def task_init(self):
        logging.info('Initializing task params: {cn_args}, {tgt_id}'.
                     format(cn_args=self.connect_args, tgt_id=self.task_id))
        self.connect_args['user'] = 'root'
        self.connect_args['password'] = 'password'
        self.connect_args['host'] = 'localhost:3306'
        #self.connect_args['host'] = '192.168.99.100:3306'
        self.connect_args['database'] = self.min15_target_db_name
        self.connect_args['table'] = self.min15_target_table_name
        self.row_col_dict['target_id'] = self.task_id

    def process_config_tmpl(self, tmpl_file):
        pcode_tz_list = Helios.get_providers_from_helios()
        self.pcode_tz_dict = dict(pcode_tz_list)
        tmpl_subst_params = {"start_time": CmvLib.date_to_cmvformat(self.start_time),
                             "end_time": CmvLib.date_to_cmvformat(self.end_time),
                             "key_space": self.key_space,
                             "name_space": self.name_space,
                             "cassandra_seeds": self.cassandra_seeds.split(','),
                             "pcode_dict": CmvLib.prepare_ptz(pcode_tz_list, list(self.hdfs_dir_set))}

        with open(tmpl_file) as json_file:
            json_data = json.load(json_file)
            CmvLib.replace_config_params(json_data, tmpl_subst_params)
            return json_data

    def prepare_js_url(self):
        js_url = 'http://{js_host}/jobs?appName={dc_jar}&classPath=ooyala.' \
                 'cnd.CreateDelphiDatacube&context={ctxt}'. \
            format(js_host=self.jobserver_host, dc_jar=self.datacube_jar, ctxt=self.context)
        return js_url

    def requires(self):
        CmvLib.check_boundaries(self.start_time)
        CmvLib.check_boundaries(self.end_time)
        cube_timeranges = set()
        now = self.start_time
        logging.info("end_time = %s", self.end_time)
        while now < self.end_time:
            logging.info("start_time = %s", self.start_time)
            logging.info("now_time = %s", now)
            cube_timeranges.add(now)
            self.hdfs_dir_set.add(now.strftime(self.hdfs_sessions+'/%Y/%m/%d/%H/%M/'))
            now += timedelta(minutes=15)

        return [InputSessionFile(cube_time=cube_time) for cube_time in cube_timeranges]

    def run(self):
        config_json = self.process_config_tmpl("/Users/jmettu/repos/wario/wario/utils/cmv_template.json")
        with open('new_config.json', 'w') as outfile:
            json.dump(config_json, outfile, indent=4)
        rslt_json = CmvLib.submit_config_to_js(config_json, self.prepare_js_url())
        job_id = rslt_json['result']['jobId']

        js_resp = CmvLib.poll_js_jobid(job_id, self.jobserver_host)

        if js_resp['status'] != 'OK':
            logging.info("Job Server responded with an error. Job Server Response: %s", js_resp)
            raise Exception('Error in Job Server Response.')
        else:
            provider_list_str = js_resp['result']['providers']
            if provider_list_str is not None:
                pcode_list = provider_list_str.replace('Set', '')[1:len(provider_list_str)-4].split(',')

        # mysql target

        ptz_dict = {str(pcode): self.pcode_tz_dict[str(pcode)] for pcode in pcode_list}

        self.row_col_dict['target_id'] = self.task_id
        self.row_col_dict['ptz_dict'] = json.dumps(ptz_dict)
        self.output().touch()

    def output(self):
        self.task_init()
        return CmvMysqlTarget(self.connect_args, self.row_col_dict)

if __name__ == '__main__':
    luigi.run(['BuildMin15Datacube', '--workers', '1', '--local-scheduler'])
    #luigi.run(['BuildMin15Datacube', '--workers', '1'])
