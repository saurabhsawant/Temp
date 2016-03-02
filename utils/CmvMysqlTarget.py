
import logging
import luigi

logger = logging.getLogger('luigi-interface')

try:
    import mysql.connector
    from mysql.connector import errorcode
except ImportError as e:
    print("things going wrong")
    logger.warning("Loading MySQL module without the python package mysql-connector-python. \
        This will crash at runtime if MySQL functionality is used.")


class CmvMySqlTarget(luigi.Target):
    """
    Target for a resource in MySql.
    """

    def __init__(self, connect_args, update_id, column_names, column_values):
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
        # "root@password@192.168.99.100:3306@luigi_poc@item_property2"

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
        self.update_id = update_id
        self.column_names = ["update_id"] + column_names
        self.column_values = [self.update_id] + column_values

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
               update_id = VALUES(update_id)
            """.format(target_table=self.table, column_names=column_names_string, values_str_fmt=values_str_fmt)

        connection.cursor().execute(insert_stmt, self.column_values)

        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True

        cursor = connection.cursor()

        try:
            cursor.execute("""SELECT 1 FROM {target_table}
                WHERE update_id = %s
                LIMIT 1""".format(target_table=self.table),
                           (self.update_id,))
            row = cursor.fetchone()
        except mysql.connector.Error as e:
            if e.errno == mysql.connector.errorcode.ER_NO_SUCH_TABLE:
                row = None
            else:
                raise
        return row is not None

    def connect(self, autocommit=False):
        connection = mysql.connector.connect(user=self.user,
                                             password=self.password,
                                             host=self.host,
                                             port=self.port,
                                             database=self.database,
                                             autocommit=autocommit)
        return connection

    def create_target_table(self):
        """
        Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect(autocommit=True)
        cursor = connection.cursor()
        try:
            cursor.execute(
                """ CREATE TABLE {marker_table} (
                        id            BIGINT(20)    NOT NULL AUTO_INCREMENT,
                        update_id     VARCHAR(128)  NOT NULL,
                        target_table  VARCHAR(128),
                        inserted      TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (update_id),
                        KEY id (id)
                    )
                """.format(marker_table=self.marker_table)
            )
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                pass
            else:
                raise
        connection.close()
