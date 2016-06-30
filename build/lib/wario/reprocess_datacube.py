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
        # TODO: add CSV validator
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


# TODO use the strategy pattern for input reading and seperate the interface for reading input
# TODO once the input is formalized, either the csv file or command line dictionary can be passed to single Task
class CmvReprocessAPIHook(luigi.Task):
    param = luigi.DictParameter()
    targets_deleted = False

    def requires(self):
        pass

    @staticmethod
    def get_next_end(start_time, cube):
        if cube == 'min15':
            return start_time + timedelta(minutes=15)
        elif cube == 'daily':
            return start_time + timedelta(days=1)
        elif cube == 'weekly':
            return start_time + timedelta(weeks=1)

    def generate_time_slice(self):
        # can be a list just to reduce a memory utilization for duplicate value in case of daily and weekly
        # as only a key will be considerd as a day for daily and weekly
        # expected values {start_time:end_time}
        time_slices = dict()

        start_time = datetime.strptime(self.param['start_time'], "%Y-%m-%dT%H%M")
        end_time = datetime.strptime(self.param['end_time'], "%Y-%m-%dT%H%M")

        while start_time <= end_time:
            temp_end_time = CmvReprocessAPIHook.get_next_end(start_time, self.param['cube'])
            time_slices[start_time] = temp_end_time

            start_time = temp_end_time

        return time_slices

    def redo_min15(self):
        cmvmin15_upstream_tasks = set()
        # iterate over the time slices dictionary {start_time:end_time} generated in sorted order
        for (start_time, end_time) in iter(sorted(self.generate_time_slice().iteritems())):
            logging.info("CmvMin15Generator task params. start_time = %s, end_time = %s", start_time, end_time)
            cmvmin15_task = CmvMin15Generator(start_time, end_time)
            cmvmin15_upstream_tasks.add(cmvmin15_task)
        return cmvmin15_upstream_tasks

    def redo_daily(self):
        daily_rollup_upstream_tasks = set()
        # iterate over the time slices dictionary {start_time:end_time} generated in sorted order
        for (start_time, end_time) in iter(sorted(self.generate_time_slice().iteritems())):
            logging.info("CmvRollupDailyGenerator task params. day = %s, pcode = %s, timezone=%s",
                         start_time.strftime("%Y-%m-%dT%H%M"), self.param['pcode'], self.param['timezone'])
            daily_rollup_task = rollup_daily.CmvRollupDailyGenerator(day=start_time, pcode=self.param['pcode'],
                                                                     timezone=self.param['timezone'])
            daily_rollup_upstream_tasks.add(daily_rollup_task)
        return daily_rollup_upstream_tasks

    def redo_weekly(self):
        weekly_rollup_upstream_tasks = set()
        # iterate over the time slices dictionary {start_time:end_time} generated in sorted order
        for (start_time, end_time) in iter(sorted(self.generate_time_slice().iteritems())):
            logging.info("CmvRollupWeeklyGenerator task params. day = %s, pcode = %s, timezone=%s",
                         start_time.strftime("%Y-%m-%dT%H%M"), self.param['pcode'], self.param['timezone'])
            weekly_rollup_task = rollup_weekly.CmvRollupWeeklyGenerator(day=start_time, pcode=self.param['pcode'],
                                                                        timezone=self.param['timezone'])
            weekly_rollup_upstream_tasks.add(weekly_rollup_task)
        return weekly_rollup_upstream_tasks

    def delete_targets(self, tasks):
        if not self.param['delete_all']:
            # Just keep the tasks for processing which are not completed
            # tasks[:] = [task for task in tasks if not task.output().exists()]  # a list result can be avoided?
            tasks = [task for task in tasks if not task.output().exists()]

        # Delete tasks o/p for all the tasks whether completed or not
        logging.info('deleting all the targets...')
        for task in tasks:
            task.output().delete()

        return tasks

    def run(self):
        reprocess_upstream_tasks = set()
        if self.param['cube'] == 'min15':
            logging.info('Reprocessing min15...')
            reprocess_upstream_tasks |= self.redo_min15()

        if self.param['cube'] == 'daily':
            reprocess_upstream_tasks |= self.redo_daily()

        if not CmvReprocessAPIHook.targets_deleted:
            reprocess_upstream_tasks = self.delete_targets(reprocess_upstream_tasks)
            CmvReprocessAPIHook.targets_deleted = True
        yield reprocess_upstream_tasks
