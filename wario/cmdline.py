import argparse
import luigi
from wario import trigger_min15_dailyrollup

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--task', required=True, nargs=1, help='Name of the task: eg Min15AndDailyRollupTrigger')
    parser.add_argument('--workers', default=3, nargs=1, help='Number of luigi workers for a given task; default: 3')
    args = parser.parse_args()
    print("~ Task: {}".format(args.task))
    print("~ Workers: {}".format(args.workers))

    if args.task[0] == 'Min15AndDailyRollupTrigger':
        task_name = trigger_min15_dailyrollup.Min15AndDailyRollupTrigger()
        luigi.run([task_name, '--workers', str(args.workers)])

