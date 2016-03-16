__author__ = 'jmettu'
import luigi
from cmvmin15 import BuildMin15Datacube
from lib.cmvlib import CmvLib
import logging
from datetime import timedelta
from cmv_mysql_target import CmvMysqlTarget

class BuildDataCube(luigi.Task):
    start_time = luigi.DateMinuteParameter()
    end_time = luigi.DateMinuteParameter()
    min15_target_table_name = luigi.Parameter(significant=False)
    min15_target_db_name = luigi.Parameter(significant=False)
    cube_time_intervals = set()

    def requires(self):
        CmvLib.check_boundaries(self.start_time)
        CmvLib.check_boundaries(self.end_time)
        now = self.start_time
        logging.info("Task: BuildDataCube, start_time = %s, end_time = %s", self.start_time, self.end_time)
        while now < self.end_time:
            self.cube_time_intervals.add((now, self.end_time))
            self.hdfs_dir_set.add(now.strftime(self.hdfs_sessions+'/%Y/%m/%d/%H/%M/'))
            now += timedelta(minutes=15)

        min15_tasks = [BuildMin15Datacube(start_time=cube_window[0], end_time=cube_window[1])
                        for cube_window in self.cube_time_intervals]

    def get_pcode_list_from_db(self, start_time, end_time):
        connect_args = dict()
        connect_args['user'] = 'root'
        connect_args['password'] = 'password'
        connect_args['host'] = '192.168.99.100:3306'
        connect_args['database'] = self.min15_target_db_name
        connect_args['table'] = self.min15_target_table_name

        query_string = """
                        select pcode_list from {min15_table} where target_id = %s
                       """.format(min15_table=self.min15_target_table_name)
        query_values = ['BuildMin15Datacube(start_time={s}),end_time={e}'.format(s=start_time, e=end_time)]

        pcode_list_string = CmvMysqlTarget(connect_args=connect_args).query(query_string, query_values)
        return pcode_list_string.split(',')

    """
    retrieve pcodes from target as a set
    """
    def get_pcode_set(self):
        pcode_lists = [self.get_pcode_list_from_db(cube_window[0], cube_window[1])
                       for cube_window in self.cube_time_intervals]
        return {pcode for pcode_list in pcode_lists for pcode in pcode_list}

    def run(self):
        pcodes = self.get_pcode_set()
        # get timezones for pcodes

    def output(self):
        pass

