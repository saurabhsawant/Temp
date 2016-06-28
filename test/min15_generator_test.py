__author__ = 'jmettu'

import unittest
import luigi
from helpers import LuigiTestCase
from wario.min15_generator import CmvMin15Generator
from datetime import datetime
from luigi.file import LocalTarget
import logging
logger = logging.getLogger('luigi-interface')

class Min15GeneratorTest(LuigiTestCase):

    def setUp(self):
        super(Min15GeneratorTest, self).setUp()
        self.scheduler = luigi.scheduler.CentralPlannerScheduler(prune_on_get_work=False)
        self.worker = luigi.worker.Worker(scheduler=self.scheduler)

    def run_task(self, task):
        self.worker.add(task)
        self.worker.run()

    def summary_dict(self):
        return luigi.execution_summary._summary_dict(self.worker)

    def summary(self):
        return luigi.execution_summary.summary(self.worker)

    def test_all_statuses(self):

        class TestMin15Generator(luigi.Task):
            start_time = datetime.strptime('2016-01-17T0045', '%Y-%m-%dT%H%M')
            end_time = datetime.strptime('2016-01-17T0100', '%Y-%m-%dT%H%M')

            def requires(self):
                return CmvMin15Generator(start_time=self.start_time,
                                         end_time=self.end_time)
            def run(self):
                f = self.output().open('w')
                f.write('exists')

            def output(self):
                return LocalTarget(path='test/resources/targets/TestMin15Generator.dep')

        logging.info('@@@ testing logger')
        self.run_task(TestMin15Generator())
        #self.run_locally(['TestMin15Generator'])
        d = self.summary_dict()
        print d


if __name__ == '__main__':
    logger.info('@@@ running locally with args')

    unittest.main()












