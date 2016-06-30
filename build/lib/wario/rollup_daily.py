"""Rollup daily task"""

from datetime import timedelta

import luigi

from wario.min15_generator import CmvMin15Generator
from wario.lib.cmvlib import DayUtcMin15Iter
from wario.rollup_base import CmvRollupBaseTask

class CmvRollupDailyGenerator(CmvRollupBaseTask):
    """Task for rollup daily"""
    wario_target_table_name = luigi.Parameter(default='rollup_day', significant=False)

    def requires(self):
        cmvmin15s = []
        for min15 in DayUtcMin15Iter(self.day, self.timezone):
            cmvmin15s.append(CmvMin15Generator(start_time=min15, end_time=min15+timedelta(minutes=15)))
        return cmvmin15s

    def get_start_time(self):
        return self.day

    def get_end_time(self):
        return self.day + timedelta(days=1)

    def get_rdd_duration(self):
        return 'min15'

    def get_rdd_rolled_duration(self):
        return 'day'

if __name__ == '__main__':
    luigi.run(['CmvRollupDailyGenerator', '--workers', '1'])
