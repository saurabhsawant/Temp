__author__ = 'jmettu'

import luigi
import csv
from datetime import timedelta
import cmvmin15
import daily_rollup

class InputReprocessFile(luigi.ExternalTask):
    reprocess_cube = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('wario/utils/reprocess_%s.csv'.format(self.reprocess_cube))


class CmvReprocess(luigi.Task):
    reprocess_min15 = luigi.BoolParameter()
    reprocess_daily = luigi.BoolParameter()
    reprocess_weekly = luigi.BoolParameter()
    reprocess_monthly = luigi.BoolParameter()

    def requires(self):
        reqd_targets = []
        if self.reprocess_min15:
            reqd_targets.append(InputReprocessFile(reprocess_cube='min15'))
        if self.reprocess_daily:
            reqd_targets.append(InputReprocessFile(reprocess_cube='daily'))
        if self.reprocess_weekly:
            reqd_targets.append(InputReprocessFile(reprocess_cube='weekly'))
        if self.reprocess_monthly:
            reqd_targets.append(InputReprocessFile(reprocess_cube='monthly'))
        return reqd_targets

    def redo_min15(self, redo_file_path):
        redo_file = open(redo_file_path)
        reader = csv.reader(redo_file)
        for line in reader:
            start_time = line[0].strftime('%Y-%m-%dT%H%M')
            end_time = start_time + timedelta(minutes=15)
            cmvmin15_task = cmvmin15(start_time, end_time)
            cmvmin15_task.output().delete()
            yield cmvmin15

    def redo_daily(self, redo_file_path):
        redo_file = open(redo_file_path)
        reader = csv.reader(redo_file)
        for line in reader:
            day = line[0].strftime('%Y-%m-%d')
            pcode = line[1]
            tz = line[2]
            daily_rollup_task = daily_rollup(day=day, pcode=pcode, timezone=tz)
            daily_rollup_task.output().delete()
            yield daily_rollup_task

    def run(self):
        if self.reprocess_min15:
            '''TODO: add CSV validator'''
            self.redo_min15('wario/utils/reprocess_min15.csv')
        if self.reprocess_daily:
            '''TODO: add CSV validator'''
            self.redo_daily('wario/utils/reprocess_daily.csv')


