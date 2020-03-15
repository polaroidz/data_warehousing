# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import json
import re
import os

from past.builtins import unicode

JOB_CONFIG_FILEPATH = './job.json'

parser = argparse.ArgumentParser()
parser.add_argument(
  '--runner',
  dest='runner',
  default='local',
  choices=['local', 'gcp', 'spark'],
  help='The runner used to execute the Apache Beam Pipeline.')

args, argv = parser.parse_known_args()

if not os.path.exists(JOB_CONFIG_FILEPATH):
    raise("Job description file (job.json) not found")

with open(JOB_CONFIG_FILEPATH, "r") as f:
    config = f.read()
    config = json.loads(config)

def _config(key):
    if not key in config:
        raise Exception("Please, add the {} on the job.json file before continue".format(key))
    return config[key]

def run_local():
    job_args = []
    
    job_args.append("--runner=DirectRunner")
    job_args.append("--job_name=" + _config("job_name"))

    import job
    #job.run(config, job_args, argv, True)
    print("Aeeeeeeehhh")

def run_gcp():
    gcp_path = _config("gcp_bucket")
    
    job_args = []
    
    job_args.append("--runner=DataflowRunner")
    job_args.append("--project=" + _config("gcp_project"))
    job_args.append("--job_name=" + _config("job_name"))
    job_args.append("--region=" + _config("gcp_region"))
    job_args.append("--temp_location=" + "{}/temp".format(gcp_path))
    job_args.append("--staging_location=" + "{}/staging".format(gcp_path))
    job_args.append("--setup_file=./setup.py")

    import job
    job.run(config, job_args, argv, True)

def run_spark():
    pass

def config_airflow():
    pass

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    if _config("airflow"):
        config_airflow()

    if args.runner == "gcp":
        run_gcp()
    elif args.runner == "spark":
        run_spark()
    else:
        run_local()
