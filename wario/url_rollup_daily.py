#
# __author__ = 'baohua'
#
"""Url rollup daily task"""

from datetime import datetime, timedelta

import luigi

from wario.url_min15 import UrlMin15Generator
from wario.url_rollup_base import UrlRollupBaseTask

class UrlRollupDailyGenerator(UrlRollupBaseTask):
    """Task for url rollup daily"""
    wario_target_table_name = luigi.Parameter(default='url_rollup_day', significant=False)

    def requires(self):
        urlmin15s = []
        date_minute = datetime(year=self.day.year, month=self.day.month, day=self.day.day)
        start_time = date_minute - timedelta(hours=12)
        end_time = date_minute + timedelta(days=1, hours=12)
        while start_time != end_time:
            urlmin15s.append(UrlMin15Generator(start_time=start_time))
            start_time += timedelta(minutes=15)
        return urlmin15s

    def get_start_time(self):
        return self.day

    def get_end_time(self):
        return self.day + timedelta(days=1)

    def get_rdd_duration(self):
        return 'min15'

    def get_rdd_rolled_duration(self):
        return 'day'

if __name__ == '__main__':
    luigi.run(['UrlRollupDailyGenerator', '--workers', '1'])
