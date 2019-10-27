#!/usr/bin/env python
# encoding: utf-8

"""Azkaban python library."""

__all__ = ['Project', 'Job', 'PigJob']
__version__ = '0.9.13'

try:
  from .ext.pig import PigJob
  from .job import Job
  from .project import Project
except ImportError:
  pass # in setup.py

import logging as lg


# docopt arguments are made available here by the CLI
CLI_ARGS = {}

class NullHandler(lg.Handler):

  """For python <2.7."""

  def emit(self, record):
    pass


lg.getLogger(__name__).addHandler(NullHandler())
