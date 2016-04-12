#
# __author__ = 'baohua'
#
"""Rollup weekly task"""

from datetime import timedelta

import luigi

from rollup_base import CmvRollupBaseTask
from rollup_daily import CmvRollupDailyGenerator
from lib.cmvlib import DateTime

class CmvRollupWeeklyGenerator(CmvRollupBaseTask):
    """Task for rollup weekly"""
    wario_target_table_name = luigi.Parameter(default='rollup_week', significant=False)

    def requires(self):
        daily_rollups = []
        day = self.get_start_time()
        end_day = self.get_end_time()
        while day != end_day:
            daily_rollups.append(
                CmvRollupDailyGenerator(day=day, pcode=self.pcode, timezone=self.timezone)
            )
            day = day + timedelta(days=1)
        return daily_rollups

    def validate_day(self):
        DateTime.validate_weekday(self.day)

    def get_start_time(self):
        self.validate_day()
        return self.day

    def get_end_time(self):
        self.validate_day()
        return self.day + timedelta(days=7)

    def get_rdd_duraion(self):
        return 'day'

    def get_rdd_rolled_duration(self):
        return 'week'

if __name__ == '__main__':
    luigi.run(['CmvRollupWeeklyGenerator', '--workers', '1'])
