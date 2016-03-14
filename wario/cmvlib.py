__author__ = 'jmettu'
import requests
import json
import time
from cmv_mysql_target import CmvMysqlTarget

import logging
logger = logging.getLogger('luigi-interface')


class Helios:

    @staticmethod
    def get_providers_from_helios():
        connect_args = dict()
        connect_args['user'] = 'helios'
        connect_args['password'] = 'JHOibizmY07I7G31'
        connect_args['host'] = "slavedb-lb01.services.ooyala.net"
        connect_args['database'] = 'vstreams'
        connect_args['table'] = None

        query_string = """
            select P.pcode as PCODE, T.iana_name as TIMEZONE
            from providers as P, timezones as T
            where P.timezone_id=T.id and P.status = %s
            """
        query_values = ['live']
        return CmvMysqlTarget(connect_args=connect_args).query(query_string, query_values)

class CmvLib:

    @staticmethod
    def check_boundaries(date_time):
        if date_time.minute % 15 != 0:
            raise ValueError('Given time %s is not at 15 min boundary' % date_time)

    @staticmethod
    def replace_config_params(json_data, tmpl_subst_params):
        for k in json_data:
            key_type = type(json_data[k])
            if key_type is dict:
                CmvLib.replace_config_params(json_data[k], tmpl_subst_params)
            elif key_type is unicode and json_data[k].startswith('$'):
                json_data[k] = tmpl_subst_params[json_data[k][1:]]

    @staticmethod
    def prepare_ptz(pcode_tz_rows, file_list):
        pcode_info = {}
        for pcode_tz in pcode_tz_rows:
            pcode_info[pcode_tz[0]] = {'numOfPartition':1, 'input-paths':[], 'timezone':pcode_tz[1]}
        if len(pcode_info) > 0:
            pcode_info.values()[0]['input-paths'] = file_list
        return pcode_info

    @staticmethod
    def submit_config_to_js(config_json, js_url):
        headers = {'content-type': 'application/json'}
        logging.info("Submitting jobserver config to url: %s", js_url)
        r = requests.post(js_url, json.dumps(config_json), headers)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def date_to_cmvformat(dt):
        return '{y}-{mo}-{d}T{h}:{mi}Z'.format(y=dt.year, mo=dt.month, d=dt.day, h=dt.hour, mi=int(dt.minute/15)*15)

    @staticmethod
    def poll_js_jobid(job_id, js_host):
        while True:
            js_resp = requests.get('http://{js_host}/jobs/{job_id}'
                                   .format(js_host=js_host, job_id=job_id)).json()
            if js_resp['status'] != 'RUNNING':
                return js_resp
            time.sleep(30)



