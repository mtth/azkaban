#!/usr/bin/env python
# encoding: utf-8

"""Azkaban project configuration script."""

from azkaban import Job, PigJob, Project
from getpass import getuser


project = Project('foo', root=__file__)

defaults = {
  'user.to.proxy': getuser(),
  'mapred': {
    'max.split.size': 2684354560,
    'min.split.size': 2684354560,
  },
}

project.add_job(
  'first_pig_script',
  PigJob(
    'path/to/first_script.pig', # assume it exists
    defaults,
  )
)

project.add_job(
  'second_pig_script',
  PigJob(
    'path/to/second_script.pig', # assume it also exists
    defaults,
    {'mapred.job.queue.name': 'special'},
  )
)

project.add_job(
  'final_job',
  Job(
    defaults,
    {
      'type': 'noop',
      'dependencies': 'first_pig_script,second_pig_script',
    }
  )
)
