from cmv_utils import pretty_json
import json
import re
import requests
import time

JOBSERVER_HOST = 'jobserver-next-staging3.services.ooyala.net'
JOBSERVER_PORT = 8090
JOBSERVER_CONTEXT = 'next-staging'
JOB_JAR_NAME = 'datacubeMaster'
JOB_STATUS_CHECK_DELAY_IN_SECONDS =  5
JOB_STATUS_CHECK_INTERVAL_IN_SECONDS = 10

def run_jobserver_job(job_class, job_cfg):
    cmd = str('http://%s:%d/jobs?appName=%s&classPath=%s&context=%s&timeout=100&sync=false' %
              (JOBSERVER_HOST, JOBSERVER_PORT, JOB_JAR_NAME, job_class, JOBSERVER_CONTEXT))
    print("Running job....\n" + cmd)
    print(pretty_json(job_cfg))
    r = requests.post(cmd, data=json.dumps(job_cfg))
    if r.status_code != requests.codes.accepted:
        return False, "Job not accepted: " + r.text
    resp = r.json()
    if not 'jobId' in resp['result']:
        return False, "Job id not found: " + r.text
    job_id = resp['result']['jobId']
    if not job_id:
        return False, "Job id is invalid: " + r.text

    status_cmd = str('http://%s:%d/jobs/%s' % (JOBSERVER_HOST, JOBSERVER_PORT, job_id))
    print(status_cmd)
    sleep_seconds = JOB_STATUS_CHECK_DELAY_IN_SECONDS
    for i in range(10000):
        time.sleep(sleep_seconds)
        sleep_seconds = JOB_STATUS_CHECK_INTERVAL_IN_SECONDS
        r = requests.get(status_cmd)
        if r.status_code != requests.codes.ok:
            continue
        resp = r.json()
        if not 'status' in resp:
            continue
        status = resp['status']
        if status != "RUNNING":
            return status == "OK", resp
    return False, re.sub("\n", "|", r.text)
