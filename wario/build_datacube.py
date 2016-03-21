__author__ = 'jmettu'
import luigi
import logging
import json
from datetime import timedelta
from lib.cmvlib import CmvLib
from lib.cmv_mysql_target import CmvMysqlTarget
from lib.cmvlib import DateTime
from cmvmin15 import BuildMin15Datacube
from daily_rollup import DailyRollup

class BuildDataCube(luigi.Task):
    start_time = luigi.DateMinuteParameter()
    end_time = luigi.DateMinuteParameter()
    min15_target_table_name = luigi.Parameter(significant=False)
    min15_target_db_name = luigi.Parameter(significant=False)
    build_datcube_target_table = luigi.Parameter(significant=False)
    build_datcube_db_table = luigi.Parameter(significant=False)
    cube_time_intervals = set()
    connect_args = dict()
    row_col_dict = dict()

    def requires(self):
        CmvLib.check_boundaries(self.start_time)
        CmvLib.check_boundaries(self.end_time)
        now = self.start_time
        logging.info("Task: BuildDataCube, start_time = %s, end_time = %s", self.start_time, self.end_time)
        while now < self.end_time:
            end = now + timedelta(minutes=15)
            self.cube_time_intervals.add((now, end))
            now = end

        return [BuildMin15Datacube(start_time=cube_window[0], end_time=cube_window[1])
                for cube_window in self.cube_time_intervals]

    def task_init(self):
        logging.info('Initializing task params: {cn_args}, {tgt_id}'.
                     format(cn_args=self.connect_args, tgt_id=self.task_id))
        self.connect_args['user'] = 'root'
        self.connect_args['password'] = ''
        self.connect_args['host'] = 'localhost:3306'
        self.connect_args['database'] = self.build_datcube_db_table
        self.connect_args['table'] = self.build_datcube_target_table
        self.row_col_dict['target_id'] = self.task_id

    def get_ptz_dict_from_db(self):
        connect_args = dict()
        connect_args['user'] = 'root'
        connect_args['password'] = ''
        connect_args['host'] = 'localhost:3306'
        connect_args['database'] = self.min15_target_db_name
        connect_args['table'] = self.min15_target_table_name

        query_string = 'select JSON_EXTRACT(ptz_dict, {json_item}) from {min15_table} where target_id = %s'.\
            format(json_item='\'$.ptz_items\'', min15_table=self.min15_target_table_name)

        query_values = ['BuildMin15Datacube(start_time={s}, end_time={e})'.
                            format(s=self.start_time.strftime('%Y-%m-%dT%H%M'),
                                   e=self.end_time.strftime('%Y-%m-%dT%H%M'))]

        rows = CmvMysqlTarget(connect_args=connect_args).query(query_string, query_values)

        return json.loads(str(rows[0][0]))

    def run(self):
        ptz_dict_list = self.get_ptz_dict_from_db()
        upstream_rollup_tasks = []
        for ptz_dict in ptz_dict_list:
            print (ptz_dict['pcode'], ptz_dict['timezone'])
            rollup_pcode = ptz_dict['pcode']
            rollup_tz = ptz_dict['timezone']
            rollup_day = DateTime.utc_to_any_tz(self.start_time, rollup_tz)
            logging.info('Preparing DailyRollup with params: day = {day}, timezone = {tz}, pcode = {pcode}'.
                         format(day=rollup_day, tz=rollup_tz, pcode=rollup_pcode))
            upstream_rollup_tasks.append(DailyRollup(day=rollup_day, timezone=rollup_tz, pcode=rollup_pcode))
        logging.info ('Triggering upstream rollup tasks')
        yield upstream_rollup_tasks
        self.output().touch()

    def output(self):
        self.task_init()
        return CmvMysqlTarget(self.connect_args, self.row_col_dict)

if __name__ == '__main__':
    luigi.run(['BuildDataCube', '--workers', '2'])


