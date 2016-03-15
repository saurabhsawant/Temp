from datetime import datetime
import json
import luigi

from cmv import Cmv
from lib.datetime_lib import date_to_utc, next_rounded_min15, next_day
from lib.mysql_lib import create_mysql_target
from lib.jobserver_lib import run_jobserver_job
from lib.cmv_utils import pretty_json

class DailyRollup(luigi.Task):
    day = luigi.DateParameter(default=datetime(year=2016,month=3,day=8))
    pcode = luigi.Parameter(default="VzcGw6NlhJZUFfutRhfdpVYIQrRp")
    timezone = luigi.Parameter(default="Asia/Kolkata", significant=False)
    cache_namespace = luigi.Parameter(default="nst_namespace", significant=False)
    cassandra_keyspace = luigi.Parameter(default="nst_keyspace", significant=False)
    cassandra_seeds = luigi.Parameter(default='cass-next-staging1.services.ooyala.net,' \
                                              'cass-next-staging2.services.ooyala.net,' \
                                              'cass-next-staging3.services.ooyala.net', significant=False)
    rollup_namespace = luigi.Parameter(default="nst-rollup", significant=False)

    def requires(self):
        if not hasattr(self, 'cmvdeps'):
            cmvdeps = []
            dateminute = date_to_utc(self.day, self.timezone)
            for i in range(0, 30):
                cmvdeps.append(Cmv(dateminute=dateminute))
                dateminute = next_rounded_min15(dateminute)
            self.cmvdeps = cmvdeps
        return self.cmvdeps

    def get_jobserver_job_class(self):
        return 'ooyala.cnd.RollupDelphiDatacubes'

    def get_jobserver_job_config(self):
        datefmt = "%Y-%m-%dT%H:%M"
        with open("utils/rollup_cfg.json") as f:
            cfg = json.load(f)
            rmv = cfg['rookery']['materialized']['view']
            rmv['cache_namespace'] = self.cache_namespace
            rmv['rollup_namespace'] = self.rollup_namespace
            rmv['pcodes'] = {
                self.pcode: {
                    "numOfPartitions": 1,
                    "timezone": self.timezone
                }
            }
            rmv['cache']['cassandra']['keyspace'] = self.cassandra_keyspace
            rmv['cache']['cassandra']['seeds'] = self.parse_cassandra_seeds(self.cassandra_seeds)
            events = cfg['rookery']['events']
            events['rdd_duration'] = 'min15'
            events['start_time'] = self.day.strftime(datefmt)
            events['end_time'] = next_day(self.day).strftime(datefmt)
            events['rdd_rollup_duration'] = "day"
            return cfg

    def parse_cassandra_seeds(self, seeds):
        return seeds.split(",")

    def run(self):
        cfg = self.get_jobserver_job_config()
        ok, output = run_jobserver_job(self.get_jobserver_job_class(), cfg)
        if not ok:
            raise Exception('Rollup job failed:', json.dumps(cfg) + "\n" + output)
        else:
            print(pretty_json(output))
        self.output().touch()

    def output(self):
        datefmt = "%Y-%m-%d"
        day_str = self.day.strftime(datefmt)
        update_id = self.pcode + ':day:' + day_str
        return create_mysql_target(update_id, day_str)
