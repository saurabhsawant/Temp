__author__ = 'jmettu'

from lib.cmvlib import CmvBaseTask
from lib.cmvlib import CmvLib
from lib.cmv_mysql_target import CmvMysqlTarget
from datetime import timedelta
from rollup_daily import CmvRollupDailyGenerator
from rollup_weekly import CmvRollupWeeklyGenerator
import luigi
import logging

class WeekupRollupTrigger(CmvBaseTask):
    day = luigi.DateParameter()
    daily_target_table_name = luigi.Parameter(significant=False)
    wario_target_table_name = luigi.Parameter(significant=False)
    connect_args = dict()
    row_col_dict = dict()
    ptz_rows = []
    start_time = day
    end_time = start_time + timedelta(days=6)

    def task_init(self):
        logging.info('Initializing task params: {cn_args}, {tgt_id}'.
                     format(cn_args=self.connect_args, tgt_id=self.task_id))
        self.connect_args['user'] = self.wario_target_db_user
        self.connect_args['password'] = self.wario_target_db_password
        self.connect_args['host'] = self.wario_target_db_host
        self.connect_args['database'] = self.wario_target_db_name
        self.row_col_dict['target_id'] = self.task_id

    def get_ptz_from_target(self):
        self.connect_args['table'] = self.daily_target_table_name
        query_string = 'select pcode, timezone from rollup_day_table ' \
                       'where day >= %s and day <= %s group by pcode, timezone'. \
            format(rollup_day_table=self.daily_target_table_name)

        query_values = [self.start_time, self.end_time]

        return CmvMysqlTarget(connect_args=self.connect_args).query(query_string, query_values)

    def requires(self):
        CmvLib.validate_weekday(self.day)
        logging.info("Task: WeekupRollupTrigger, start_time = %s, end_time = %s", self.start_time, self.end_time)
        self.ptz_rows = self.get_ptz_from_target()
        daily_rollup_tasks = []
        for ptz in self.ptz_rows:
            for curr_day in (self.start_time + timedelta(days=n) for n in range(7)):
                daily_rollup_tasks.append(CmvRollupDailyGenerator(day=curr_day, pcode=ptz[0], timezone=ptz[1]))
        return daily_rollup_tasks

    def run(self):
        for ptz in self.ptz_rows:
            yield CmvRollupWeeklyGenerator(day=self.start_time, pcode=ptz[0], timezone=ptz[1])

    def output(self):
        self.task_init()
        self.connect_args['table'] = self.wario_target_table_name
        return CmvMysqlTarget(self.connect_args, self.row_col_dict)
