#
# __author__ = 'baohua'
#
"""Rollup monthly task"""

from datetime import date, timedelta

import luigi

from rollup_base import CmvRollupBaseTask
from rollup_daily import CmvRollupDailyGenerator
from rollup_weekly import CmvRollupWeeklyGenerator
from lib.cmvlib import DateTime

class CmvRollupMonthlyGenerator(CmvRollupBaseTask):
    """Task for rollup monthly"""
    wario_target_table_name = luigi.Parameter(default='rollup_month', significant=False)

    def requires(self):
        rollups = []
        day = self.get_start_time()
        end_day = self.get_end_time()
        while day != end_day:
            if day.weekday() == 0 and (day + timedelta(days=7)) <= end_day:
                rollups.append(
                    CmvRollupWeeklyGenerator(day=day, pcode=self.pcode, timezone=self.timezone)
                )
                day = day + timedelta(days=7)
            else:
                rollups.append(
                    CmvRollupDailyGenerator(day=day, pcode=self.pcode, timezone=self.timezone)
                )
                day = day + timedelta(days=1)
        return rollups

    def validate_day(self):
        DateTime.validate_start_of_month(self.day)

    def get_start_time(self):
        self.validate_day()
        return self.day

    def get_end_time(self):
        self.validate_day()
        next_month_day = self.day + timedelta(days=date.max.day)
        end_day = next_month_day - timedelta(days=(next_month_day.day - 1))
        return end_day

    def get_rdd_duraion(self):
        return 'day'

    def get_rdd_rolled_duration(self):
        return 'month'

if __name__ == '__main__':
    luigi.run(['CmvRollupMonthlyGenerator', '--workers', '1'])
