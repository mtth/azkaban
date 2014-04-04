#!/usr/bin/env python
# encoding: utf-8

"""Azkaban example project configuration script.

Let us assume we have a flow with many pig scripts to run, which share many
options. This example shows a way to concisely generate the project.

"""

from azkaban import Job, PigJob, Project
from getpass import getuser


project = Project('bar', root=__file__)

defaults = {
  'user.to.proxy': getuser(),
  'mapred': {
    'max.split.size': 2684354560,
    'min.split.size': 2684354560,
  },
}

# dictionary of pig script options, keyed on the pig script path
pig_options = {
  'first.pig': {},
  'second.pig': {'dependencies': 'first.pig'},
  'third.pig': {'pig.param': {'foo': 48}},
  'fourth.pig': {'dependencies': 'first.pig,third.pig'},
}

for path, options in pig_options.items():
  project.add_job(path, PigJob(path, defaults, options))
