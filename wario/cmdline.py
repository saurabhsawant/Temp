import argparse
import luigi
from datetime import datetime, timedelta
from wario.min15_generator import CmvMin15Generator
from wario.url_min15 import UrlMin15Generator
from wario.url_rollup_daily import UrlRollupDailyGenerator
from wario.trigger_min15_dailyrollup import Min15AndDailyRollupTrigger
from wario.trigger_weekly_rollup import WeeklyRollupTrigger
from wario.trigger_monthly_rollup import MonthlyRollupTrigger
from wario.reprocess_datacube import CmvReprocess

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', required=True, nargs=1, help='Name of the task: eg Min15AndDailyRollupTrigger')
    parser.add_argument('--runs', default=1, type=int, help='Number of tasks to run')
    parser.add_argument('--ago', default=0, type=int, help='Minutes ago')
    parser.add_argument('--workers', default=3, type=int, help='Number of luigi workers for a given task; default: 3')
    args = parser.parse_args()

    utcnow = datetime.utcnow() - timedelta(minutes=args.ago)
    if args.task[0] in ['CmvMin15Generator',
                        'UrlMin15Generator',
                        'Min15AndDailyRollupTrigger']:
        start_time = datetime(year=utcnow.year, month=utcnow.month, day=utcnow.day,
                              hour=utcnow.hour, minute=(utcnow.minute/15) * 15)
    elif args.task[0] == 'UrlRollupDailyGenerator':
        start_time = datetime(year=utcnow.year, month=utcnow.month, day=utcnow.day)
    elif args.task[0] == 'WeeklyRollupTrigger':
        start_time = datetime(year=utcnow.year, month=utcnow.month,
                              day=utcnow.day) - timedelta(days=utcnow.wday)
    elif args.task[0] == 'MonthlyRollupTrigger':
        start_time = datetime(year=utcnow.year, month=utcnow.month,
                              day=utcnow.day) - timedelta(days=(utcnow.day - 1))
    elif args.task[0] == 'CmvReprocess':
        args.runs = 1
    else:
        raise argparse.ArgumentTypeError('''Unknown Task encountered. Task candidates are:
                                CmvMin15Generator,UrlMin15Generator,Min15AndDailyRollupTrigger,
                                UrlRollupDailyGenerator,WeeklyRollupTrigger,MonthlyRollupTrigger,
                                and CmvReprocess''')


