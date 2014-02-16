#!/usr/bin/env python
# encoding: utf-8

"""Azkaban python library.

Two parts:

* Azkaban CLI: a lightweight command line interface for Azkaban.

* Azkaban module: a simple way to define jobs.

  Sample usage:

    from azkaban import Job, Project

    project = Project('foo')

    project.add_job('bar', Job())
    project.add_file('/some/file.path')

"""

__all__ = ['Project', 'Job', 'PigJob']
__version__ = '0.3.0'

try:
  from .ext.pig import PigJob
  from .job import Job
  from .project import Project
except ImportError:
  pass # in setup.py

import logging


class NullHandler(logging.Handler):

  """For python <2.7."""

  def emit(self, record):
    pass

logging.getLogger(__name__).addHandler(NullHandler())
