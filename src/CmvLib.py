__author__ = 'jmettu'

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
