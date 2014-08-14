#!/usr/bin/env python
# encoding: utf-8

"""Azkaban python library."""

__all__ = ['Project', 'Job', 'PigJob']
__version__ = '0.6.41'

try:
  from .ext.pig import PigJob
  from .job import Job
  from .project import Project
except ImportError:
  pass # in setup.py

import logging as lg


class NullHandler(lg.Handler):

  """For python <2.7."""

  def emit(self, record):
    pass


lg.getLogger(__name__).addHandler(NullHandler())
