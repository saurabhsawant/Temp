__author__ = 'jmettu'

from lib.cmvlib import CmvBaseTask
from lib.cmvlib import DateTime
from lib.cmv_mysql_target import CmvMysqlTarget
from datetime import timedelta
from rollup_daily import CmvRollupDailyGenerator
from rollup_weekly import CmvRollupWeeklyGenerator
import luigi
import logging

class MonthlyRollupTrigger(CmvBaseTask):
    day = luigi.DateParameter()
    daily_target_table_name = luigi.Parameter(significant=False)
    wario_target_table_name = luigi.Parameter(significant=False)
    connect_args = dict()
    row_col_dict = dict()
    ptz_rows = []
    start_time = None
    end_time = None

    def task_init(self):
        self.connect_args['user'] = self.wario_target_db_user
        self.connect_args['password'] = self.wario_target_db_password
        self.connect_args['host'] = self.wario_target_db_host
        self.connect_args['database'] = self.wario_target_db_name
        self.connect_args['table'] = self.wario_target_table_name
        self.row_col_dict['target_id'] = self.task_id
        logging.info('Initializing task params: {cn_args}, {tgt_id}'.
                     format(cn_args=self.connect_args, tgt_id=self.task_id))

    def get_ptz_from_target(self):
        self.connect_args['table'] = self.daily_target_table_name
        query_string = 'select pcode, timezone from rollup_day ' \
                       'where day >= %s and day <= %s group by pcode, timezone'. \
            format(rollup_day_table=self.daily_target_table_name)

        query_values = [self.start_time, self.end_time]

        return CmvMysqlTarget(connect_args=self.connect_args).query(query_string, query_values)

    def requires(self):
        DateTime.validate_start_of_month(self.day)
        logging.info("Task: %s, start_time = %s, end_time = %s", self.__class__.__name__, self.start_time, self.end_time)
        self.start_time = self.day
        self.end_time = self.start_time + timedelta(days=6)
        self.ptz_rows = self.get_ptz_from_target()
        daily_rollup_tasks = []
        for ptz in self.ptz_rows:
            for curr_day in (self.start_time + timedelta(days=n) for n in range(7)):
                daily_rollup_tasks.append(CmvRollupDailyGenerator(day=curr_day, pcode=ptz[0], timezone=ptz[1]))
        return daily_rollup_tasks

    def run(self):
        for ptz in self.ptz_rows:
            yield CmvRollupWeeklyGenerator(day=self.start_time, pcode=ptz[0], timezone=ptz[1])
        self.output().touch()

    def output(self):
        self.task_init()
        return CmvMysqlTarget(self.connect_args, self.row_col_dict)
