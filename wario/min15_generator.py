from lib.cmvlib import *
import json
from datetime import timedelta
import luigi
import logging
import time
from lib.cmvlib import DataDogClient


class CmvMin15Generator(CmvBaseTask):
    start_time = luigi.DateMinuteParameter()
    end_time = luigi.DateMinuteParameter()
    wario_target_table_name = luigi.Parameter(default='min15', significant=False)
    appserver_app_name = luigi.Parameter(default='min15-gen', significant=False)
    appserver_app_type = luigi.Parameter(default='min15', significant=False)
    hdfs_dir_set = set()
    provider_list_str = None
    connect_args = dict()
    column_formats = dict()
    pcode_tz_dict = dict()

    row_col_dict = dict()
    row_col_dict['target_id'] = None

    def task_init(self):
        logging.info('Initializing task params: {cn_args}, {tgt_id}'.
                     format(cn_args=self.connect_args, tgt_id=self.task_id))

        self.connect_args['user'] = self.wario_target_db_user
        self.connect_args['password'] = self.wario_target_db_password
        self.connect_args['host'] = self.wario_target_db_host
        self.connect_args['database'] = self.wario_target_db_name
        self.connect_args['table'] = self.wario_target_table_name
        self.row_col_dict['target_id'] = self.task_id
        self.column_formats = {'ptz_dict': "column_create('ptz_items', %s)"}

    def process_config_tmpl(self, tmpl_file):
        pcode_tz_list = Helios.get_providers_from_helios()
        self.pcode_tz_dict = dict(pcode_tz_list)
        hdfs_dirs = [hdfs_dir.path.rsplit('/', 1)[0] for hdfs_dir in self.input()]

        tmpl_subst_params = {"start_time": CmvLib.date_to_cmvformat(self.start_time),
                             "end_time": CmvLib.date_to_cmvformat(self.end_time),
                             "key_space": self.cassandra_keyspace,
                             "name_space": self.cassandra_namespace,
                             "cassandra_seeds": self.cassandra_seeds.split(','),
                             "pcode_dict": CmvLib.prepare_ptz(pcode_tz_list, hdfs_dirs)}

        with open(tmpl_file) as json_file:
            json_data = json.load(json_file)
            CmvLib.replace_config_params(json_data, tmpl_subst_params)
            return json_data

    def requires(self):
        CmvLib.validate_min15_time(self.start_time)
        CmvLib.validate_min15_time(self.end_time)
        cube_timeranges = set()
        now = self.start_time
        logging.info("start_time = %s, end_time = %s", self.start_time, self.end_time)
        while now < self.end_time:
            cube_timeranges.add(now)
            now += timedelta(minutes=15)
        return [InputSessionFile(cube_time=cube_time) for cube_time in cube_timeranges]

    def run(self):

        config_json = self.process_config_tmpl(CmvLib.get_template_path('resources/cmv_template.json'))
        with open('new_config.json', 'w') as outfile:
            json.dump(config_json, outfile, indent=4)
        datadog_start_time = time.time()
        appserver_jobsubmit_url = CmvLib.get_appserver_job_submit_url(self.appserver_host_port,
                                                                      self.appserver_app_name,
                                                                      self.appserver_app_type)
        rslt_json = CmvLib.submit_config_to_appserver(config_json, appserver_jobsubmit_url)

        job_id = rslt_json['payload']['jobId']
        appserver_jobstatus_url = CmvLib.get_appserver_job_status_url(self.appserver_host_port,
                                                                      self.appserver_app_name,
                                                                      job_id)
        appserver_resp = CmvLib.poll_appserver_job_status(appserver_jobstatus_url)
        DataDogClient.gauge_this_metric('min15_delay', (time.time()-datadog_start_time))

        if appserver_resp['payload']['status'] != 'Finished':
            logging.error("AppServer responded with an error. AppServer Response: %s",
                          appserver_resp['payload']['result'])
            raise Exception('Error in Appserver Response.')
        else:
            provider_list_str = appserver_resp['payload']['result']['result']['providers']
            if provider_list_str is not None:
                pcode_list = provider_list_str.replace('Set', '')[1:len(provider_list_str)-4].split(',')

        ptz_list = []
        for pcode in pcode_list:
            ptz_dict_item = dict()
            if not pcode or str(pcode).lstrip() == 'unknown':
                continue
            ptz_dict_item['pcode'] = str(pcode).lstrip()
            ptz_dict_item['timezone'] = self.pcode_tz_dict[str(pcode).lstrip()]
            ptz_list.append(ptz_dict_item)
        DataDogClient.gauge_this_metric('min15_provider_count', len(ptz_list))
        self.row_col_dict['target_id'] = self.task_id
        self.row_col_dict['ptz_dict'] = json.dumps(ptz_list)
        self.output().touch()

    def output(self):
        self.task_init()
        return CmvMysqlTarget(self.connect_args, self.row_col_dict, column_formats=self.column_formats)

if __name__ == '__main__':
    luigi.run(['CmvMin15Generator', '--workers', '1'])
