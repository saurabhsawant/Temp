__author__ = 'jmettu'
import luigi
import logging
import json
from lib.cmvlib import CmvLib
from lib.cmvlib import CmvBaseTask
from lib.cmv_mysql_target import CmvMysqlTarget
from lib.cmvlib import DateTime
from cmvmin15 import BuildMin15Datacube
from daily_rollup import DailyRollup

class BuildDataCube(CmvBaseTask):
    start_time = luigi.DateMinuteParameter()
    end_time = luigi.DateMinuteParameter()
    min15_target_table_name = None
    wario_target_table_name = luigi.Parameter(significant=False)
    cube_time_intervals = set()
    connect_args = dict()
    row_col_dict = dict()

    def requires(self):
        CmvLib.check_boundaries(self.start_time)
        CmvLib.check_boundaries(self.end_time)
        logging.info("Task: BuildDataCube, start_time = %s, end_time = %s", self.start_time, self.end_time)
        min15_task = BuildMin15Datacube(start_time=self.start_time, end_time=self.end_time)
        self.min15_target_table_name = min15_task.wario_target_table_name
        return min15_task

    def task_init(self):
        logging.info('Initializing task params: {cn_args}, {tgt_id}'.
                     format(cn_args=self.connect_args, tgt_id=self.task_id))
        self.connect_args['user'] = self.wario_target_db_user
        self.connect_args['password'] = self.wario_target_db_password
        self.connect_args['host'] = self.wario_target_db_host
        self.connect_args['database'] = self.wario_target_db_name
        self.connect_args['table'] = self.wario_target_table_name
        self.row_col_dict['target_id'] = self.task_id

    def get_ptz_dict_from_db(self):
        connect_args = dict()
        connect_args['user'] = self.wario_target_db_user
        connect_args['password'] = self.wario_target_db_password
        connect_args['host'] = self.wario_target_db_host
        connect_args['database'] = self.wario_target_db_name
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


