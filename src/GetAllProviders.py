__author__ = 'jmettu'

from utils.CmvMysqlTarget import *
logger = logging.getLogger('luigi-interface')


try:
    import mysql.connector
    from mysql.connector import errorcode
except ImportError as e:
    logger.warning("Loading MySQL module without the python package mysql-connector-python. \
        This will crash at runtime if MySQL functionality is used.")

class GetAllProviders:
    connect_args = dict()
    connect_args['user'] = 'helios'
    connect_args['password'] = 'JHOibizmY07I7G31'
    connect_args['host'] = "slavedb-lb01.services.ooyala.net"
    connect_args['database'] = 'vstreams'
    connect_args['table'] = None

    def __init__(self):
        pass

    def query_helios(self):
        query_string = """
        select P.pcode as PCODE, T.iana_name as TIMEZONE
        from providers as P, timezones as T
        where P.timezone_id=T.id and P.status = %s
        """
        query_values = ['live']
        reslt_itrtr = CmvMySqlTarget(connect_args=self.connect_args).query(query_string, query_values)

        # for row in reslt_itrtr:
        #     print row

if __name__ == '__main__':
    GetAllProviders().query_helios()

