
import logging
import luigi

logger = logging.getLogger('luigi-interface')

try:
    import mysql.connector
    from mysql.connector import errorcode
except ImportError as e:
    logger.warning("Loading MySQL module without the python package mysql-connector-python. \
        This will crash at runtime if MySQL functionality is used.")


class CmvMysqlTarget(luigi.Target):
    """
    Target for a resource in MySql.
    """

    def __init__(self, connect_args, row_col_dict=None):
        """
        Initializes a MySqlTarget instance.

        :param connect_args: connection arguments,
        :type connect_args: dictionary of strings user, password, host, database, table
        :param update_id: an identifier for this data set.
        :type update_id: str
        :param column_names: names of columns.
        :type column_names: list of strings
        :param column_values: values of columns.
        :type column_values: list of strings
        """
        if ':' in connect_args['host']:
            self.host, self.port = connect_args['host'].split(':')
            self.port = int(self.port)
        else:
            self.host = connect_args['host']
            self.port = 3306
        self.database = connect_args['database']
        self.user = connect_args['user']
        self.password = connect_args['password']
        self.table = connect_args['table']
        self.column_names = []
        self.column_values = []
        self.target_id = None
        if row_col_dict:
            self.target_id = row_col_dict['target_id']
            for col in row_col_dict:
                self.column_names.append(col)
                self.column_values.append(row_col_dict[col])

    def touch(self, connection=None):
        """
        Mark this update as complete.

        IMPORTANT, If the marker table doesn't exist,
        the connection transaction will be aborted and the connection reset.
        Then the marker table will be created.
        """

        if connection is None:
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        column_names_string = ','.join(self.column_names)
        values_str_fmt = ', '.join(["%s"] * len(self.column_names))

        insert_stmt = """INSERT INTO {target_table} ({column_names})
               VALUES ({values_str_fmt})
               ON DUPLICATE KEY UPDATE
               target_id = VALUES(target_id)
            """.format(target_table=self.table, column_names=column_names_string, values_str_fmt=values_str_fmt)

        connection.cursor().execute(insert_stmt, self.column_values)
        logging.info('Updated Target table {trgt}'.format(trgt=self.table))

        # make sure update is properly marked
        assert self.exists(connection)

    def query(self, query_string, query_values=None, connection=None):
        """
        :param query_string: query string examples:
        [1] select * from table.
        [2] SELECT * FROM foo WHERE bar = %s AND baz = %s
        :param query_values:
        [1] None
        [2] list with bar and baz values.
        :param connection: optional connection
        :return: iterator with rows
        """
        if connection is None:
            connection = self.connect()
            connection.autocommit = True

        cursor = connection.cursor()

        try:
            cursor.execute(query_string, query_values)
            return cursor.fetchall()
        except mysql.connector.Error:
            raise

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True

        cursor = connection.cursor()

        try:
            logging.info("Checking exists for table: {table}, and target_id: {target_id}".
                         format(table=self.table, target_id=self.target_id))
            cursor.execute("""SELECT 1 FROM {target_table}
                WHERE target_id = %s
                LIMIT 1""".format(target_table=self.table),
                           (self.target_id,))
            row = cursor.fetchone()
        except mysql.connector.Error as e:
            if e.errno == mysql.connector.errorcode.ER_NO_SUCH_TABLE:
                row = None
            else:
                raise
        return row is not None

    def delete(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True

        cursor = connection.cursor()

        try:
            cursor.execute("""DELETE FROM {target_table}
                WHERE target_id = %s""".format(target_table=self.table),
                           (self.target_id,))
        except mysql.connector.Error:
            raise

    def connect(self, autocommit=False):
        logging.info("Connecting to db: {db} at host: {host}, port: {port}, user: {user}".
                     format(db=self.database, host=self.host, port=self.port, user=self.user))
        connection = mysql.connector.connect(user=self.user,
                                             password=self.password,
                                             host=self.host,
                                             port=self.port,
                                             database=self.database,
                                             autocommit=autocommit)
        return connection
