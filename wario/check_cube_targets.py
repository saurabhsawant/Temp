import luigi
from datetime import timedelta, datetime
from min15_generator import CmvMin15Generator
from lib.cmvlib import CmvLib
from rollup_daily import CmvRollupDailyGenerator
from rollup_weekly import CmvRollupWeeklyGenerator


class CmvCheckCubeTargets(luigi.Task):
    start_time = luigi.DateMinuteParameter()
    end_time = luigi.DateMinuteParameter()
    task = luigi.Parameter()
    target_root = luigi.Parameter(significant=False, default='/var/log/luigi/luigi_targets/checkcubes')

    def requires(self):
        pass

    def run(self):
        now = self.start_time
        target_tasks = set()
        if self.task == 'min15':
            CmvLib.validate_min15_time(now)
            while now < self.end_time:
                ends_at = now + timedelta(minutes=15)
                cmv_task = CmvMin15Generator(start_time=now, end_time=ends_at)
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
                    f.write('{s}=completed'.format(s=task.start_time))
                else:
                    f.write('{s}=not_completed'.format(s=task.start_time))
                f.write(',')

    def output(self):
        return luigi.LocalTarget('{root}/{s}_{e}_{task}.csv'.format(root=self.target_root,
                                                                    s=datetime.strptime(self.start_time.__str__(), "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H%M"),
                                                                    e=datetime.strptime(self.end_time.__str__(), "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H%M"),
                                                                    task=self.task))
