__author__ = 'jmettu'

import luigi
import logging
from luigi.contrib.hdfs import HdfsTarget
from CmvLib import *
import json
from datetime import timedelta
import requests

class InputSessionFile(luigi.ExternalTask):
    cube_time = luigi.DateMinuteParameter()
    hdfs_sessions = luigi.Parameter()
    hdfs_namenode = luigi.Parameter()

    def output(self):
        hdfs_str = self.cube_time.strftime(self.hdfs_sessions+'/%Y/%m/%d/%H/%M/_SUCCESS')
        logging.info(hdfs_str)
        return HdfsTarget(hdfs_str)

class CmvMin15(luigi.Task):
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
    hdfs_dir_list = []

    tmpl_subst_params = {"start_time": start_time,
                         "end_time": end_time,
                         "key_space": key_space,
                         "name_space": name_space,
                         "cassandra_seeds": cassandra_seeds,
                         "pcode_dic": prepare_ptz(get_providers_from_helios(), hdfs_dir_list)}

    def process_config_tmpl(self, tmpl_file):
        with open(tmpl_file) as json_file:
            json_data = json.load(json_file)
            replace_config_params(json_data, self.tmpl_subst_params)
            return json_data

    def prepare_js_url(self):
        js_url = 'http://{js_host}/jobs?appName={dc_jar}&classPath=ooyala.' \
                 'cnd.CreateDelphiDatacube&context={ctxt}&sync=false'. \
            format(js_host=self.jobserver_host, dc_jar=self.datacube_jar, ctxt=self.context)
        return js_url

    def requires(self):
        check_boundaries(self.start_time)
        check_boundaries(self.end_time)

        cube_timeranges = []
        now = self.start_time
        logging.info("end_time = %s", self.end_time)
        while now < self.end_time:
            logging.info("start_time = %s", self.start_time)
            logging.info("now_time = %s", now)
            cube_timeranges.append(now)
            self.hdfs_dir_list.append(self.cube_time.strftime(self.hdfs_sessions+'/%Y/%m/%d/%H/%M/'))
            now += timedelta(minutes=15)

        return [InputSessionFile(cube_time=cube_time) for cube_time in cube_timeranges]

    def run(self):
        config_json = self.process_config_tmpl("/Users/jmettu/repos/analytics-workflow-service/utils/cmv_template.json")
        submit_config_to_js(config_json, self.prepare_js_url())

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget('/tmp/luigi-poc/touchme')

if __name__ == '__main__':
    luigi.run(['CmvMin15', '--workers', '1', '--local-scheduler'])
