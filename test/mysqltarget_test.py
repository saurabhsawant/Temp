__author__ = 'jmettu'
from wario.lib.cmv_mysql_target import CmvMysqlTarget

import logging
import unittest

logger = logging.getLogger('luigi-interface')
try:
    import mysql.connector
    from mysql.connector import errorcode
except ImportError as e:
    logger.warning("Loading MySQL module without the python package mysql-connector-python. \
        This will crash at runtime if MySQL functionality is used.")

host = '192.168.99.100'
port = 3306
database = 'wario_test_db'
username = 'root'
password = ''
target_table = 'wario_test_table'
test_col_name = 'target_id'
test_col_val = 'mysqltarget_test'

def create_test_database():
    con = mysql.connector.connect(user=username,
                                  password=password,
                                  host=host,
                                  port=port,
                                  autocommit=True)
    con.cursor().execute('drop database if exists %s' % database)
    con.cursor().execute('create database %s' % database)

def create_test_table():
    con = mysql.connector.connect(user=username,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database,
                                  autocommit=True)
    con.cursor().execute('create table  if not exists %s (%s TEXT NOT NULL)' % (target_table, test_col_name))


connect_args = {'user': username, 'password': password, 'database': database, 'table': target_table, 'host': host}
row_col_dict = {test_col_name: test_col_val}
target = CmvMysqlTarget(connect_args, row_col_dict)




class MySqlTargetTest(unittest.TestCase):

    def get_connection(self):
        return target.connect()

    def truncate_tables(self):
        conn = self.get_connection()
        conn.cursor().execute('truncate wario_test_table')

    def test_touch_and_exists(self):
        self.truncate_tables()
        target.touch()
        self.assertTrue(target.exists(),
                        'Target should exist after touching it')

    def test_query(self):
        self.truncate_tables()
        target.touch()
        query_result = target.query('select target_id from wario_test_table where target_id = %s', [test_col_val])
        self.assertTrue(test_col_val == query_result[0][0],
                        'Query should return expected row value')

    def test_delete(self):
        self.truncate_tables()
        target.touch()
        target.delete()
        query_result = target.query('select count(*) from wario_test_table', [])
        self.assertTrue(0 == query_result[0][0],
                        'All the rows are not deleted from the target')


if __name__ == '__main__':
    create_test_database()
    create_test_table()
    unittest.main()
