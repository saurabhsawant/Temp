__author__ = 'jmettu'
import requests
import json

from utils.CmvMysqlTarget import *
logger = logging.getLogger('luigi-interface')

def check_boundaries(date_time):
    if date_time.minute % 15 != 0:
        raise ValueError('Given time %s is not at 15 min boundary' % date_time)


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
    return CmvMySqlTarget(connect_args=connect_args).query(query_string, query_values)

def prepare_ptz(pcode_tz_rows):
    pcode_info = {}
    for pcode_tz in pcode_tz_rows:
        pcode_info[pcode_tz[0]] = {'numOfPartition':1, 'input-paths':[], 'timezone':pcode_tz[1]}
    return pcode_info

def submit_config_to_js(self, config_json, js_url):
    headers = {'content-type': 'application/json'}
    r = requests.post(js_url, json.dumps(config_json), headers)
    r.raise_for_status()
    return r.json()

