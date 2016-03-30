import argparse
import luigi
from wario.trigger_min15_dailyrollup import Min15AndDailyRollupTrigger
from wario.trigger_weekly_rollup import WeeklyRollupTrigger
from wario.trigger_monthly_rollup import MonthlyRollupTrigger
from wario.reprocess_datacube import CmvReprocess

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', required=True, nargs=1, help='Name of the task: eg Min15AndDailyRollupTrigger')
    parser.add_argument('--workers', default=3, nargs=1, help='Number of luigi workers for a given task; default: 3')
    args = parser.parse_args()

    if args.task[0] == 'Min15AndDailyRollupTrigger':
        task_name = [Min15AndDailyRollupTrigger()]
    elif args.task[0] == 'WeeklyRollupTrigger':
        task_name = [WeeklyRollupTrigger()]
    elif args.task[0] == 'MonthlyRollupTrigger':
        task_name = [MonthlyRollupTrigger()]
    elif args.task[0] == 'CmvReprocess':
        task_name = [CmvReprocess()]
    else:
        raise argparse.ArgumentTypeError('''Unknown Task encountered. Task candidates are:
                            Min15AndDailyRollupTrigger/WeeklyRollupTrigger/MonthlyRollupTrigger/CmvReprocess''')

    luigi.build(task_name, workers=args.workers)



