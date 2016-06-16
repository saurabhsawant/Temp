import luigi
import csv
from datetime import timedelta
from min15_generator import CmvMin15Generator
import rollup_daily
import rollup_weekly
import logging
from datetime import datetime
from lib.cmvlib import DateTime

targets_deleted = False


class InputReprocessFile(luigi.ExternalTask):
    reprocess_cube = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('wario/resources/reprocess_{suffix}.csv'.format(suffix=self.reprocess_cube))


class CmvReprocess(luigi.Task):
    reprocess_min15 = luigi.BoolParameter()
    reprocess_daily = luigi.BoolParameter()
    reprocess_weekly = luigi.BoolParameter()
    reprocess_monthly = luigi.BoolParameter()

    def requires(self):
        reqd_targets = dict()
        if self.reprocess_min15:
            reqd_targets['min15'] = InputReprocessFile(reprocess_cube='min15')
        if self.reprocess_daily:
            reqd_targets['daily'] = InputReprocessFile(reprocess_cube='daily')
        if self.reprocess_weekly:
            reqd_targets['weekly'] = InputReprocessFile(reprocess_cube='weekly')
        if self.reprocess_monthly:
            reqd_targets['monthly'] = InputReprocessFile(reprocess_cube='monthly')
        return reqd_targets

    def redo_min15(self, redo_file_handler):
        reader = csv.reader(redo_file_handler)
        cmvmin15_upstream_tasks = set()
        for line in reader:
            start_time = datetime.strptime(line[0], '%Y-%m-%dT%H%M')
            end_time = start_time + timedelta(minutes=15)
            logging.info("CmvMin15 task params. start_time = %s, end_time = %s", start_time, end_time)
            cmvmin15_task = CmvMin15Generator(start_time, end_time)
            cmvmin15_upstream_tasks.add(cmvmin15_task)
        return cmvmin15_upstream_tasks

    def redo_daily(self, redo_file_handler):
        reader = csv.reader(redo_file_handler)
        daily_rollup_upstream_tasks = set()
        for line in reader:
            day = datetime.strptime(line[0], '%Y-%m-%d')
            pcode = line[1]
            tz = line[2]
            daily_rollup_task = rollup_daily.CmvRollupDailyGenerator(day=day, pcode=pcode, timezone=tz)
            daily_rollup_upstream_tasks.add(daily_rollup_task)
        return daily_rollup_upstream_tasks

    def redo_weekly(self, redo_file_handler):
        reader = csv.reader(redo_file_handler)
        weekly_rollup_upstream_tasks = set()
        for line in reader:
            day = datetime.strptime(line[0], '%Y-%m-%d')
            DateTime.validate_weekday(day)
            pcode = line[1]
            tz = line[2]
            weekly_rollup_task = rollup_weekly.CmvRollupWeeklyGenerator(day=day, pcode=pcode, timezone=tz)
            weekly_rollup_upstream_tasks.add(weekly_rollup_task)
        return weekly_rollup_upstream_tasks

    def delete_all_targets(self, all_tasks):
        logging.info('deleting all the targets...')
        [task.output().delete() for task in all_tasks]

    def run(self):
        reprocess_upstream_tasks = set()
        global targets_deleted
        #TODO: add CSV validator
        if self.reprocess_min15:
            logging.info('Reprocessing min15...')
            reprocess_upstream_tasks |= self.redo_min15(self.input()['min15'].open('r'))

        if self.reprocess_daily:
            reprocess_upstream_tasks |= self.redo_daily(self.input()['daily'].open('r'))

        if not targets_deleted:
            self.delete_all_targets(reprocess_upstream_tasks)
            targets_deleted = True
        yield reprocess_upstream_tasks

    def complete(self):
        return targets_deleted

if __name__ == '__main__':
    luigi.run(['CmvReprocess', '--workers', '1'])
