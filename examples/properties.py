#!/usr/bin/env python
# encoding: utf-8

"""Azkaban example project configuration script, using project properties.

This example shows how to simply define a project with two configurations:
production and test, without any job duplication.

"""

from azkaban import Job, Project
from getpass import getuser


# Production project
# ------------------
#
# This project is configured to run in a production environment (e.g. using a
# headless user with permissions to write to a specific directory).

PROJECT = Project('azkabancli_sample', root=__file__)
PROJECT.properties = {
  'user.to.proxy': 'production_user',
  'hdfs.root': '/jobs/sample/'
}

# dictionary of jobs, keyed by job name
JOBS = {

  'gather_data': Job({
    'type': 'hadoopJava',
    'job.class': 'sample.GatherData',
    'path.output': '${hdfs.root}data.avro', # note the property use here
  }),

  # ...

}

for name, job in JOBS.items():
  PROJECT.add_job(name, job)

# Test project
# ------------
#
# This project is an exact copy of the production project which can be used
# to debug / test new features independently from the production flows.

TEST_PROJECT = Project('sample_test', root=__file__)
TEST_PROJECT.properties = {
  'user.to.proxy': getuser(),
  'hdfs.root': 'sample/'
}
PROJECT.merge_into(TEST_PROJECT)
