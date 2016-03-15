__author__ = 'jmettu'
import luigi
from cmvmin15 import BuildMin15Datacube

class BuildDataCube(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        min15_start_time = self.date.strftime('%Y-%m-%dT0000')
        min15_end_time = self.date.strftime('%Y-%m-%dT0015')
        return BuildMin15Datacube(start_time=min15_start_time, end_time=min15_end_time)

    def run(self):
        pass

