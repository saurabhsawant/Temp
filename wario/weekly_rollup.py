__author__ = 'jmettu'
import luigi
from wario.daily_rollup import DailyRollup
from datetime import timedelta

class WeeklyRollup(DailyRollup):
    start_day = luigi.Parameter()
    end_day = luigi.Parameter()
    pcode = luigi.Parameter()
    timezone = luigi.parameter()
    rollup_target_table_name = luigi.Parameter(significant=False)

    def validate_dates(self, start_day, end_day):
        pass

    def requires(self):
        #validate_dates(self.start_day, self.end_day,)
        daily_rollups = []
        curr_date = self.start_day
        while curr_date < self.end_day:
            daily_rollups.append(DailyRollup(day=curr_date,pcode = self.pcode,timezone=self.timezone))
            curr_date = curr_date + timedelta(days=1)







