__author__ = 'jmettu'
import luigi
import logging
import json
from lib.cmvlib import CmvLib
from lib.cmvlib import CmvBaseTask
from lib.cmv_mysql_target import CmvMysqlTarget
from lib.cmvlib import DateTime
from url_min15 import UrlMin15Generator
from url_rollup_daily import UrlRollupDailyGenerator

class UrlMin15AndDailyRollupTrigger(CmvBaseTask):
    start_time = luigi.DateMinuteParameter()
    end_time = luigi.DateMinuteParameter()
    min15_target_table_name = None
    wario_target_table_name = luigi.Parameter(significant=False)
    connect_args = dict()
    row_col_dict = dict()

    def requires(self):
        CmvLib.validate_min15_time(self.start_time)
        CmvLib.validate_min15_time(self.end_time)
        logging.info("Task: %s, start_time = %s, end_time = %s", self.__class__.__name__, self.start_time, self.end_time)
        min15_task = UrlMin15Generator(start_time=self.start_time, end_time=self.end_time)
        self.min15_target_table_name = min15_task.wario_target_table_name
        return min15_task

    def task_init(self):
        self.connect_args['user'] = self.wario_target_db_user
        self.connect_args['password'] = self.wario_target_db_password
        self.connect_args['host'] = self.wario_target_db_host
        self.connect_args['database'] = self.wario_target_db_name
        self.connect_args['table'] = self.wario_target_table_name
        self.row_col_dict['target_id'] = self.task_id
        logging.info('Initializing task params: {cn_args}, {tgt_id}'.
                     format(cn_args=self.connect_args, tgt_id=self.task_id))

    def get_ptz_dict_from_db(self):
        connect_args = dict()
        connect_args['user'] = self.wario_target_db_user
        connect_args['password'] = self.wario_target_db_password
        connect_args['host'] = self.wario_target_db_host
        connect_args['database'] = self.wario_target_db_name
        connect_args['table'] = self.min15_target_table_name

        query_string = 'select COLUMN_GET(ptz_dict, {json_item} as char) from {min15_table} where target_id = %s'. \
            format(json_item='\'ptz_items\'', min15_table=self.min15_target_table_name)

        query_values = ['UrlMin15Generator(start_time={s}, end_time={e})'.
                            format(s=self.start_time.strftime('%Y-%m-%dT%H%M'), e=self.end_time.strftime('%Y-%m-%dT%H%M'))]

        rows = CmvMysqlTarget(connect_args=connect_args).query(query_string, query_values)

        return json.loads(str(rows[0][0]))

    def run(self):
        ptz_dict_list = self.get_ptz_dict_from_db()
        upstream_rollup_tasks = []
        for ptz_dict in ptz_dict_list:
            rollup_pcode = ptz_dict['pcode']
            rollup_tz = ptz_dict['timezone']
            rollup_day = DateTime.utc_to_any_tz(self.start_time, rollup_tz).replace(tzinfo=None)
            logging.info('Preparing DailyRollup with params: day = {day}, timezone = {tz}, pcode = {pcode}'.
                         format(day=rollup_day, tz=rollup_tz, pcode=rollup_pcode))
            upstream_rollup_tasks.append(UrlRollupDailyGenerator(day=rollup_day,
                                                                 timezone=rollup_tz,
                                                                 pcode=rollup_pcode))
        logging.info('Triggering upstream rollup tasks')
        yield upstream_rollup_tasks
        self.output().touch()

    def output(self):
        self.task_init()
        return CmvMysqlTarget(self.connect_args, self.row_col_dict)

if __name__ == '__main__':
    luigi.run(['UrlMin15AndDailyRollupTrigger', '--workers', '2'])


