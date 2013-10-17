#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI.

Usage:
  python FILE build PATH
  python FILE upload URL
  python FILE view

Arguments:
  FILE            Jobs file.
  PATH            Output path where zip file will be created.
  URL             Azkaban endpoint.

Options:
  -h --help       Show this message and exit.
  -v --version    Show version and exit.

"""

from ConfigParser import NoOptionError, RawConfigParser
from contextlib import contextmanager
from docopt import docopt
from getpass import getpass, getuser
from os import close, remove
from os.path import basename, exists, expanduser, isabs, join, splitext
from requests import post
from sys import argv
from tempfile import mkstemp
from zipfile import ZipFile


__version__ = '0.0.1'


def flatten(dct, sep='.'):
  """Flatten a nested dictionary.

  :param dct: dictionary to flatten.
  :param sep: separator used when concatenating keys.

  """
  def _flatten(dct, prefix=''):
    """Inner recursive function."""
    items = []
    for key, value in dct.items():
      new_prefix = '%s%s%s' % (prefix, sep, key) if prefix else key
      if isinstance(value, dict):
        items.extend(_flatten(value, new_prefix).items())
      else:
        items.append((new_prefix, value))
    return dict(items)
  return _flatten(dct)

@contextmanager
def temppath():
  """Create a temporary filepath.

  Usage::

    with temppath() as path:
      # do stuff

  Any file corresponding to the path will be automatically deleted afterwards.

  """
  (desc, path) = mkstemp()
  close(desc)
  remove(path)
  try:
    yield path
  finally:
    if exists(path):
      remove(path)


class AzkabanError(Exception):

  """Base error class."""


class Project(object):

  """Azkaban project.

  :param name: name of the project
  :param user: username which will be used to upload the built project
    (defaults to the current user)

  """

  def __init__(self, name, user=None):
    self.name = name
    self.user = user or getuser()
    self._jobs = {}
    self._files = {}

  def add_file(self, path, archive_path=None):
    """Include a file in the project archive.

    :param path: absolute path to file
    :param archive_path: path to file in archive (defaults to same as `path`)

    This method requires the path to be absolute to avoid having files in the
    archive with lower level destinations than the base root directory.

    """
    if not isabs(path):
      raise AzkabanError('relative path not allowed: %r' % (path, ))
    if path in self._files:
      if self._files[path] != archive_path:
        raise AzkabanError('inconsistent duplicate: %r' % (path, ))
    else:
      if not exists(path):
        raise AzkabanError('file missing: %r' % (path, ))
      self._files[path] = archive_path

  def add_job(self, name, job):
    """Include a job in the project.

    :param name: name assigned to job (must be unique)
    :param job: `Job` subclass

    This method triggers the `on_add` method on the added job (passing the
    project and name as arguments). The handler will be called right after the
    job is added.

    """
    if name in self._jobs:
      raise AzkabanError('duplicate job name: %r' % (name, ))
    else:
      self._jobs[name] = job
      job.on_add(self, name)

  def build(self, path):
    """Create the project archive.

    :param path: destination path

    Triggers the `on_build` method on each job inside the project (passing
    itself and the job's name as two argument). This method will be called
    right before the job file is generated.

    """
    with ZipFile(path, 'w') as writer:
      for name, job in self._jobs.items():
        job.on_build(self, name)
        with temppath() as fpath:
          job.generate(fpath)
          writer.write(fpath, '%s.job' % (name, ))
      for fpath, apath in self._files.items():
        writer.write(fpath, apath)

  def upload(self, url):
    """TODO: Build and upload project to Azkaban.

    :param url: http endpoint (including port)

    """
    with temppath() as path:
      self.build(path)
      url = get_cluster_url(cluster, url)
      session_id = get_session_id(cluster, self.user)
      req = post(
        '%s/manager' % (url, ),
        data={
          'ajax': 'upload',
          'session-id': session_id,
          'project': self.name,
          'file': path
        }
      )

  def run(self):
    """TODO: Command line interface."""
    argv.insert(0, 'FILE')
    args = docopt(__doc__, version=__version__)
    if args['build']:
      self.build(args['PATH'])
    elif args['upload']:
      pass
    elif args['view']:
      for name in self._jobs:
        print name


class Job(object):

  """Base Azkaban job.

  :param options: list of dictionaries (earlier values take precedence).

  To enable more functionality, subclass and override the `on_add` and
  `on_build` methods.

  """

  def __init__(self, *options):
    self._options = options

  @property
  def options(self):
    """Combined job options."""
    options = {}
    for option in reversed(self._options):
      options.update(flatten(option))
    return options

  def generate(self, path):
    """Create job file.

    :param path: path where job file will be created. Any existing file will
      be overwritten.

    """
    with open(path, 'w') as writer:
      for key, value in sorted(self.options.items()):
        writer.write('%s=%s\n' % (key, value))

  def on_add(self, project, name):
    """Handler called when the job is added to a project.

    :param project: project instance
    :param name: name corresponding to this job in the project.

    """
    pass

  def on_build(self, project, name):
    """Handler called when a project including this job is built.

    :param project: project instance
    :param name: name corresponding to this job in the project.

    """
    pass


class PigJob(Job):

  """Job class corresponding to pig jobs.

  :param path: path to pig script
  :param *options: cf. `Job`

  Implements helpful handlers. To use custom pig type jobs, override the `type`
  class attribute.

  TODO: automatic dependency detection using variables.

  """

  type = 'pig'

  def __init__(self, path, *options):
    if not exists(path):
      raise AzkabanError('pig script missing: %r' % (path, ))
    super(PigJob, self).__init__(
      {'type': self.type, 'pig.script': path},
      *options
    )
    self.path = path

  def on_add(self, project, name):
    """Adds script file to project."""
    project.add_file(self.path)
