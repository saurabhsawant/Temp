from luigi.contrib.mysqldb import MySqlTarget

MYSQL_HOST = 'localhost'
MYSQL_DATABASE = 'cmvworkflow'
MYSQL_USER = 'root'
MYSQL_PASSWORD = ''

# Creates a mysql target
def create_mysql_target(update_id, target_table):
    return MySqlTarget(MYSQL_HOST, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD,
                       target_table, update_id)