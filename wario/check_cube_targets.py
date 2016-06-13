import luigi
from datetime import timedelta
from min15_generator import CmvMin15Generator
from lib.cmvlib import CmvLib
from rollup_daily import CmvRollupDailyGenerator
from rollup_weekly import CmvRollupWeeklyGenerator


class CmvCheckCubeTargets(luigi.Task):
    start_time = luigi.DateMinuteParameter()
    end_time = luigi.DateMinuteParameter()
    task = luigi.Parameter()
    target_root = luigi.Parameter(significant=False, default='/var/log/luigi_targets/checkcubes')

    def requires(self):
        pass

    def run(self):
        now = self.start_time
        target_tasks = set()
        if self.task == 'min15':
            CmvLib.validate_min15_time(now)
            while now < self.end_time:
                ends_at = now + timedelta(minutes=15)
                cmv_task = CmvMin15Generator(now, ends_at)
                now = ends_at
                target_tasks.add(cmv_task)
        elif self.task == 'day':
            while now < self.end_time:
                ends_at = now + timedelta(days=1)
                cmv_task = CmvRollupDailyGenerator(now, ends_at)
                now = ends_at
                target_tasks.add(cmv_task)
        elif self.task == 'week':
            now = now - timedelta(days=now.weekday)
            while now < self.end_time:
                ends_at = now + timedelta(weeks=1)
                cmv_task = CmvRollupWeeklyGenerator(now, ends_at)
                now = ends_at
                target_tasks.add(cmv_task)
        else:
            raise ValueError("Incorrect task name provided. Should be one among min15, day, or week")

        with self.output().open('w') as f:
            for task in target_tasks:
                if task.output().exists():
                    f.write('%s,completed', task.start_time)
                else:
                    f.write('%s,not_completed', task.start_time)

    def output(self):
        return luigi.LocalTarget('%s/%s_%s_%s.csv', self.target_root, self.start_time, self.end_time, self.task)
