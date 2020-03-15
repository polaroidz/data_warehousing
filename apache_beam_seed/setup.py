import setuptools
import json
import os

JOB_CONFIG_FILEPATH = './job.json'

with open(JOB_CONFIG_FILEPATH, "r") as f:
    config = f.read()
    config = json.loads(config)

REQUIRED_PACKAGES = []
PACKAGE_NAME = config["job_name"]
PACKAGE_VERSION = config["version"]
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description=config["description"],
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
)