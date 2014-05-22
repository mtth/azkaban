#!/usr/bin/env python
# encoding: utf-8

"""Azkaban sample project configuration script.

Let us assume we have a flow with pig scripts to run, which share many
options. This example shows a way to concisely build the project.

"""

from azkaban import PigJob, Project
from getpass import getuser


PROJECT = Project('azkabancli_sample', root=__file__)

# default options for all jobs
DEFAULTS = {
  'user.to.proxy': getuser(),
  'param': {
    'input_root': 'sample_dir/',
    'n_reducers': 20,
  },
  'jvm.args.mapred': {
    'max.split.size': 2684354560,
    'min.split.size': 2684354560,
  },
}

# list of pig job options
OPTIONS = [
  {'pig.script': 'first.pig'},
  {'pig.script': 'second.pig', 'dependencies': 'first.pig'},
  {'pig.script': 'third.pig', 'param': {'foo': 48}},
  {'pig.script': 'fourth.pig', 'dependencies': 'second.pig,third.pig'},
]

for option in OPTIONS:
  PROJECT.add_job(option['pig.script'], PigJob(DEFAULTS, option))
