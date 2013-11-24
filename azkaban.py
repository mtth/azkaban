#!/usr/bin/env python
# encoding: utf-8

"""Azkaban CLI.

Usage:
  python FILE upload [-q] (-a ALIAS | [-u USER] URL)
  python FILE build [-fq] PATH
  python FILE view JOB
  python FILE list
  python FILE -h | --help | -v | --version

Commmands:
  upload                        Upload project to Azkaban server.
  build                         Build zip archive.
  view                          View job options.
  list                          View list of jobs.

Arguments:
  FILE                          Project configuration file.
  JOB                           Job name.
  PATH                          Output path where zip file will be created.
  URL                           Azkaban endpoint (with protocol).

Options:
  -a ALIAS --alias=ALIAS        Alias to saved URL and username. Will also try
                                to reuse session IDs for later connections.
  -f --force                    Overwrite any existing file.
  -h --help                     Show this message and exit.
  -q --quiet                    Suppress output.
  -u USER --user=USER           Username used to log into Azkaban (defaults to
                                the current user, as determined by `whoami`).
  -v --version                  Show version and exit.

"""

from collections import defaultdict
from ConfigParser import RawConfigParser
from contextlib import contextmanager
from getpass import getpass, getuser
from os import close, remove
from os.path import exists, expanduser, getsize, isabs
from sys import argv, exit, stdout
from tempfile import mkstemp
from zipfile import ZipFile

try:
  from docopt import docopt
  from requests import post, ConnectionError
  from requests.exceptions import MissingSchema
except ImportError:
  pass

import logging

__version__ = '0.1.8'


class NullHandler(logging.Handler):

  """For python <2.7."""

  def emit(self, record):
    pass

logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())


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

def human_readable(size):
  """Transform size from bytes to human readable format (kB, MB, ...).

  :param size: size in bytes

  """
  for suffix in ['bytes', 'kB', 'MB', 'GB', 'TB']:
    if size < 1024.0:
      return '%3.1f%s' % (size, suffix)
    size /= 1024.0

def pretty_print(info):
  """Prints pretty representation of dictionary to stdout.

  :param info: dictionary

  """
  keys = sorted(info.keys())
  padding = max(len(key) for key in keys)
  header_format = '%%%ss: %%s\n' % (padding + 1, )
  content_format = ' ' * (padding + 3) + '%s\n'
  for key in keys:
    value = info[key]
    if isinstance(value, list):
      options = sorted(value)
      stdout.write(header_format % (key, options[0]))
      for option in options[1:]:
        stdout.write(content_format % (option, ))
    else:
      stdout.write(header_format % (key, value))

def get_formatted_stream_handler():
  """Returns a formatted stream handler used for the command line parser."""
  handler = logging.StreamHandler()
  formatter = logging.Formatter('%(levelname)s: %(message)s')
  handler.setFormatter(formatter)
  return handler

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

  """

  rcpath = expanduser('~/.azkabanrc')

  def __init__(self, name):
    self.name = name
    self._jobs = {}
    self._files = {}

  def add_file(self, path, archive_path=None):
    """Include a file in the project archive.

    :param path: absolute path to file
    :param archive_path: path to file in archive (defaults to same as `path`)

    This method requires the path to be absolute to avoid having files in the
    archive with lower level destinations than the base root directory.

    """
    logger.debug('adding file %r as %r', path, archive_path or path)
    if not isabs(path):
      raise AzkabanError('relative path not allowed %r' % (path, ))
    elif path in self._files:
      if self._files[path] != archive_path:
        raise AzkabanError('inconsistent duplicate %r' % (path, ))
    else:
      if not exists(path):
        raise AzkabanError('missing file %r' % (path, ))
      self._files[path] = archive_path

  def add_job(self, name, job):
    """Include a job in the project.

    :param name: name assigned to job (must be unique)
    :param job: `Job` subclass

    This method triggers the `on_add` method on the added job (passing the
    project and name as arguments). The handler will be called right after the
    job is added.

    """
    logger.debug('adding job %r', name)
    if name in self._jobs:
      raise AzkabanError('duplicate job name %r' % (name, ))
    else:
      self._jobs[name] = job
      job.on_add(self, name)

  def merge(self, project):
    """Merge one project with another.

    :param project: project to merge with this project

    This method does an in place merge of the current project with another.
    The merged project will maintain the current project's name.
    """
    logger.debug('merging project %r with %r', self.name, project.name)
    for name, job in project._jobs.items():
      self.add_job(name, job)

    for path, archive_path in project._files.items():
      self.add_file(path, archive_path)

  def build(self, path, force=False):
    """Create the project archive.

    :param path: destination path

    Triggers the `on_build` method on each job inside the project (passing
    itself and the job's name as two argument). This method will be called
    right before the job file is generated.

    """
    logger.debug('building project')
    # not using a with statement for compatibility with older python versions
    if exists(path) and not force:
      raise AzkabanError('path %r already exists' % (path, ))
    if not (len(self._jobs) or len(self._files)):
      raise AzkabanError('building empty project')
    writer = ZipFile(path, 'w')
    try:
      for name, job in self._jobs.items():
        job.on_build(self, name)
        with temppath() as fpath:
          job.build(fpath)
          writer.write(fpath, '%s.job' % (name, ))
      for fpath, apath in self._files.items():
        writer.write(fpath, apath)
    finally:
      writer.close()
    size = human_readable(getsize(path))
    logger.info('project successfully built (size: %s)' % (size, ))

  def upload(self, url=None, user=None, password=None, alias=None):
    """Build and upload project to Azkaban.

    :param url: http endpoint URL (including protocol)
    :param user: Azkaban username (must have the appropriate permissions)
    :param password: Azkaban login password
    :param alias: section of rc file used to cache URLs (will enable session
      ID caching)

    Note that in order to upload to Azkaban, the project must have already been
    created and the corresponding user must have permissions to upload.

    """
    (url, session_id) = self._get_credentials(url, user, password, alias)
    logger.debug('uploading project to %r', url)
    with temppath() as path:
      self.build(path)
      try:
        req = post(
          '%s/manager' % (url, ),
          data={
            'ajax': 'upload',
            'session.id': session_id,
            'project': self.name,
          },
          files={
            'file': ('file.zip', open(path, 'rb'), 'application/zip'),
          },
          verify=False
        )
      except ConnectionError:
        raise AzkabanError('unable to connect to azkaban server')
      except MissingSchema:
        raise AzkabanError('invalid azkaban server url')
      else:
        res = req.json()
        if 'error' in res:
          raise AzkabanError(res['error'])
        else:
          logger.info(
            'project successfully uploaded (id: %s, version: %s)' %
            (res['projectId'], res['version'])
          )
          return res

  def main(self):
    """Command line argument parser."""
    argv.insert(0, 'FILE')
    args = docopt(__doc__, version=__version__)
    if not args['--quiet']:
      logger.setLevel(logging.INFO)
      logger.addHandler(get_formatted_stream_handler())
    try:
      if args['build']:
        self.build(args['PATH'], force=args['--force'])
      elif args['upload']:
        self.upload(
          url=args['URL'],
          user=args['--user'],
          alias=args['--alias'],
        )
      elif args['view']:
        job_name = args['JOB']
        if job_name in self._jobs:
          job = self._jobs[job_name]
          pretty_print(job.build_options)
        else:
          raise AzkabanError('missing job %r' % (job_name, ))
      elif args['list']:
        jobs = defaultdict(list)
        for name, job in self._jobs.items():
          job_type = job.build_options.get('type', '--')
          job_deps = job.build_options.get('dependencies', '')
          if job_deps:
            info = '%s [%s]' % (name, job_deps)
          else:
            info = name
          jobs[job_type].append(info)
        pretty_print(jobs)
    except AzkabanError as err:
      logger.error(err)
      exit(1)

  def _get_credentials(self, url=None, user=None, password=None, alias=None):
    """Get valid session ID.

    :param url: http endpoint (including port)
    :param user: username which will be used to upload the built project
      (defaults to the current user)
    :param password: password used to log into Azkaban
    :param alias: alias name used to find the URL, and an existing
      session ID if possible (will override the URL parameter)

    """
    if alias:
      parser = RawConfigParser({'user': '', 'session_id': ''})
      parser.read(self.rcpath)
      if not parser.has_section(alias):
        raise AzkabanError('missing alias %r' % (alias, ))
      elif not parser.has_option(alias, 'url'):
        raise AzkabanError('missing url for alias %r' % (alias, ))
      else:
        url = parser.get(alias, 'url')
        user = parser.get(alias, 'user')
        session_id = parser.get(alias, 'session_id')
    elif url:
      session_id = None
    else:
      raise ValueError('Either url or alias must be specified.')
    url = url.rstrip('/')
    if not session_id or post(
      '%s/manager' % (url, ),
      {'session.id': session_id},
      verify=False
    ).text:
      user = user or getuser()
      password = password or getpass('azkaban password for %s: ' % (user, ))
      try:
        req = post(
          url,
          data={'action': 'login', 'username': user, 'password': password},
          verify=False,
        )
      except ConnectionError:
        raise AzkabanError('unable to connect to azkaban server')
      except MissingSchema:
        raise AzkabanError('invalid azkaban server url')
      else:
        res = req.json()
        if 'error' in res:
          raise AzkabanError(res['error'])
        else:
          session_id = res['session.id']
          if alias:
            parser.set(alias, 'session_id', session_id)
            with open(self.rcpath, 'w') as writer:
              parser.write(writer)
    return (url, session_id)


class Job(object):

  """Base Azkaban job.

  :param options: list of dictionaries (later values take precedence).

  To enable more functionality, subclass and override the `on_add` and
  `on_build` methods.

  """

  def __init__(self, *options):
    self.options = options

  @property
  def build_options(self):
    """Combined job options."""
    options = {}
    for option in self.options:
      options.update(flatten(option))
    for key, value in options.items():
      if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        logger.warn('non-standard value %r for option %r', value, key)
      options[key] = str(value)
    return options

  def build(self, path):
    """Create job file.

    :param path: path where job file will be created. Any existing file will
      be overwritten.

    """
    with open(path, 'w') as writer:
      for key, value in sorted(self.build_options.items()):
        writer.write('%s=%s\n' % (key, value))

  def on_add(self, project, name):
    """Handler called when the job is added to a project.

    :param project: project instance
    :param name: name corresponding to this job in the project.

    The default implementation does nothing.

    """
    pass

  def on_build(self, project, name):
    """Handler called when a project including this job is built.

    :param project: project instance
    :param name: name corresponding to this job in the project.

    The default implementation does nothing.

    """
    pass


class PigJob(Job):

  """Job class corresponding to pig jobs.

  :param path: absolute path to pig script (this script will automatically be
    added to the project archive)
  :param options: cf. `Job`

  """

  #: Job type used (change this to use a custom pig type).
  type = 'pig'

  def __init__(self, path, *options):
    if not exists(path):
      raise AzkabanError('missing pig script %r' % (path, ))
    super(PigJob, self).__init__(
      {'type': self.type, 'pig.script': path.lstrip('/')},
      *options
    )
    self.path = path

  def on_add(self, project, name):
    """This handler adds the corresponding script file to the project."""
    project.add_file(self.path)
