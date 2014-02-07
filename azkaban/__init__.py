#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI: a lightweight command line interface for Azkaban.

Usage:
  python FILE upload [-qz ZIP] (-a ALIAS | [-u USER] URL)
  python FILE run [-q] (-a ALIAS | [-u USER] URL) FLOW
  python FILE build [-oq] PATH
  python FILE view JOB
  python FILE list [-fp]
  python FILE -h | --help | -v | --version

Commmands:
  upload                        Upload project to Azkaban server.
  run                           Run workflow.
  build                         Build zip archive.
  view                          View job options.
  list                          View list of jobs.

Arguments:
  FILE                          Project configuration file.
  FLOW                          Workflow (job without children) name.
  JOB                           Job name.
  PATH                          Output path where zip file will be created.
  URL                           Azkaban endpoint (with protocol).

Options:
  -a ALIAS --alias=ALIAS        Alias to saved URL and username. Will also try
                                to reuse session IDs for later connections.
  -f --files                    List project files instead of jobs.
  -h --help                     Show this message and exit.
  -p --pretty                   Organize jobs by type and show dependencies. If
                                used with the `--files` option, will show the
                                size of each and its path in the archive.
  -o --overwrite                Overwrite any existing file.
  -q --quiet                    Suppress output. The return status of the
                                command will still signal errors.
  -u USER --user=USER           Username used to log into Azkaban (defaults to
                                the current user, as determined by `whoami`).
  -v --version                  Show version and exit.
  -z ZIP --zip=ZIP              Path to existing zip archive. If specified,
                                this file will be directly uploaded to
                                Azkaban (skipping the project build).

"""

__version__ = '0.2.0'


import logging

try:
  from .project import Project
  from .job import Job, PigJob
except ImportError:
  pass # in setup.py


class NullHandler(logging.Handler):

  """For python <2.7."""

  def emit(self, record):
    pass


logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())
