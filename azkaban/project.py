#!/usr/bin/env python
# encoding: utf-8

"""Project definition module."""


from ConfigParser import RawConfigParser
from getpass import getpass, getuser
from os import sep
from os.path import (basename, dirname, exists, expanduser, getsize, isabs,
  splitext, join)
from sys import path as sys_path
from weakref import WeakValueDictionary
from zipfile import ZipFile

from .util import AzkabanError, temppath, azkaban_request, extract_json

import logging


logger = logging.getLogger(__name__)


class EmptyProject(object):

  """Azkaban project.

  :param name: project name

  This class is never meant to be instantiated from a user's code. It is only
  used by the command line interface to execute commands on the server which
  do not require building a project.

  """

  rcpath = expanduser('~/.azkabanrc')

  def __init__(self, name):
    self.name = name

  def __repr__(self):
    return '<%s %r>' % (self.__class__, self.name)

  def create(self, description, url, session_id):
    """Create a new project on Azkaban.

    :param description: description of the project
    :param url: http endpoint URL (including protocol)
    :param session_id: Azkaban session ID

    """
    res = extract_json(azkaban_request(
      'POST',
      '%s/manager' % (url, ),
      data={
        'action': 'create',
        'name': self.name,
        'description': description,
      },
      cookies={
        'azkaban.browser.session.id': session_id,
      },
    ))
    return res

  def delete(self, url, session_id):
    """Delete a project on Azkaban.

    :param url: http endpoint URL (including protocol)
    :param session_id: Azkaban session ID

    """
    res = azkaban_request(
      'GET',
      '%s/manager' % (url, ),
      params={
        'project': self.name,
        'delete': 'true',
      },
      cookies={
        'azkaban.browser.session.id': session_id,
      },
    )
    success = "Project '%s' was successfully deleted" % (self.name, )
    if success in res.text:
      return res
    else:
      raise AzkabanError('Delete failed. Check permissions and existence.')
    return res

  def run(self, flow, url, session_id, jobs=None, block=False):
    """Run a workflow on Azkaban.

    :param flow: name of the workflow
    :param url: http endpoint URL (including protocol and user name)
    :param session_id: Azkaban session ID
    :param jobs: name of jobs to run (run entire workflow by default)
    :param block: don't run if the same workflow is already running

    Note that in order to run a workflow on Azkaban, it must already have been
    uploaded and the corresponding user must have permissions to run.

    """
    if not jobs:
      logger.debug('running flow %r on %r', flow, url)
      disabled = '[]'
    else:
      logger.debug('running jobs %r of flow %r on %r', jobs, flow, url)
      all_names = set(self.get_flow_jobs(flow, url, session_id))
      run_names = set(jobs)
      missing_names = run_names - all_names
      if missing_names:
        raise AzkabanError(
          'Jobs not found in flow %r: %s.' %
          (flow, ', '.join(missing_names))
        )
      else:
        disabled = (
          '[%s]'
          % (','.join('"%s"' % (n, ) for n in all_names - run_names), )
        )
    res = extract_json(azkaban_request(
      'POST',
      '%s/executor' % (url, ),
      data={
        'ajax': 'executeFlow',
        'session.id': session_id,
        'project': self.name,
        'flow': flow,
        'disabled': disabled,
        'concurrentOption': 'skip' if block else 'concurrent',
      },
    ))
    logger.debug('azkaban response: %r' % (res, ))
    return res

  def upload(self, archive, url, session_id):
    """Build and upload project to Azkaban.

    :param archive: path to zip file (typically the output of `build`)
    :param url: http endpoint URL (including protocol)
    :param session_id: Azkaban session ID

    Note that in order to upload to Azkaban, the project must have already been
    created and the corresponding user must have permissions to upload.

    """
    logger.debug('uploading project to %r', url)
    if not exists(archive):
      raise AzkabanError('Unable to find archive at %r.' % (archive, ))
    res = extract_json(azkaban_request(
      'POST',
      '%s/manager' % (url, ),
      data={
        'ajax': 'upload',
        'session.id': session_id,
        'project': self.name,
      },
      files={
        'file': ('file.zip', open(archive, 'rb'), 'application/zip'),
      },
    ))
    logger.debug('azkaban response: %r' % (res, ))
    return res

  def get_flow_jobs(self, flow, url, session_id):
    """Get list of jobs corresponding to flow on Azkaban server.

    :param flow: name of flow in project
    :param url: Azkaban server endpoint
    :param session_id: valid session id

    """
    logger.debug('finding jobs for flow %r on %r', flow, url)
    raw_res = azkaban_request(
      'GET',
      '%s/manager' % (url, ),
      params={
        'ajax': 'fetchflowjobs',
        'project': self.name,
        'flow': flow,
      },
      cookies={
        'azkaban.browser.session.id': session_id,
      },
    )
    logger.debug('azkaban response: %r' % (raw_res, ))
    try:
      res = extract_json(raw_res)
    except ValueError:
      raise AzkabanError('Flow %r not found.' % (flow, ))
    else:
      return [n['id'] for n in res['nodes']]

  def get_session(self, url=None, password=None, alias=None):
    """Get URL and associated valid session ID.

    :param url: http endpoint (including port and optional user)
    :param password: password used to log into Azkaban (only used if no alias
      is provided)
    :param alias: alias name used to find the URL, user, and an existing
      session ID if possible (will override the `url` parameter)

    """
    if alias:
      parser = RawConfigParser({'user': '', 'session_id': ''})
      parser.read(self.rcpath)
      if not parser.has_section(alias):
        raise AzkabanError('Missing alias %r.' % (alias, ))
      elif not parser.has_option(alias, 'url'):
        raise AzkabanError('Missing url for alias %r.' % (alias, ))
      else:
        url = parser.get(alias, 'url')
        user = parser.get(alias, 'user')
        session_id = parser.get(alias, 'session_id')
    elif url:
      session_id = None
      parsed_url = url.split('@')
      parsed_url_length = len(parsed_url)
      if parsed_url_length == 1:
        user = getuser()
        url = parsed_url[0]
      elif parsed_url_length == 2:
        user = parsed_url[0]
        url = parsed_url[1]
      else:
        raise AzkabanError('Malformed url: %r' % (url, ))
    else:
      # value error since this is never supposed to happen when called by the
      # CLI (handled by docopt)
      raise ValueError('Either url or alias must be specified.')
    url = url.rstrip('/')
    user = user or getuser()
    if not session_id or azkaban_request(
      'POST',
      '%s/manager' % (url, ),
      data={'session.id': session_id},
    ).text:
      password = password or getpass('azkaban password for %s: ' % (user, ))
      res = extract_json(azkaban_request(
        'POST',
        url,
        data={'action': 'login', 'username': user, 'password': password},
      ))
      session_id = res['session.id']
      if alias:
        parser.set(alias, 'session_id', session_id)
        with open(self.rcpath, 'w') as writer:
          parser.write(writer)
    return {'url': url, 'session_id': session_id}


class Project(EmptyProject):

  """Azkaban project.

  :param name: name of the project
  :param register: add project to registry. setting this to false will make it
    invisible to the CLI

  """

  _registry = WeakValueDictionary()

  def __init__(self, name, register=True, root=None):
    super(Project, self).__init__(name)
    self.root = dirname(root) if root else None
    self._jobs = {}
    self._files = {}
    if register:
      self._registry[name] = self

  @property
  def jobs(self):
    """Returns a dictionary with each job options.

    This property should not be used to add jobs. Use `add_job` instead.

    """
    return dict(
      (name, job.options)
      for name, job in self._jobs.items()
    )

  def add_file(self, path, archive_path=None):
    """Include a file in the project archive.

    :param path: absolute path to file
    :param archive_path: path to file in archive (defaults to same as `path`)

    This method requires the path to be absolute to avoid having files in the
    archive with lower level destinations than the base root directory.

    """
    logger.debug('adding file %r as %r', path, archive_path or path)
    if not isabs(path):
      if self.root:
        path = join(self.root, path)
      else:
        raise AzkabanError(
          'Relative path not allowed without specifying a project root: %r.'
          % (path, )
        )
    if path in self._files:
      if self._files[path] != archive_path:
        raise AzkabanError('Inconsistent duplicate file: %r.' % (path, ))
    else:
      if not exists(path):
        raise AzkabanError('File not found: %r.' % (path, ))
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
      raise AzkabanError('Duplicate job name: %r.' % (name, ))
    else:
      self._jobs[name] = job
      job.on_add(self, name)

  def merge_into(self, project):
    """Merge one project with another.

    :param project: project to merge with this project

    This method does an in place merge of the current project with another.
    The merged project will maintain the current project's name.
    """
    logger.debug('merging into project %r', project.name)
    for name, job in self._jobs.items():
      project.add_job(name, job)
    for path, archive_path in self._files.items():
      project.add_file(path, archive_path)

  def build(self, path, overwrite=False):
    """Create the project archive.

    :param path: destination path
    :param overwrite: don't throw an error if a file already exists at `path`

    Triggers the `on_build` method on each job inside the project (passing
    itself and the job's name as two argument). This method will be called
    right before the job file is generated.

    """
    logger.debug('building project')
    # not using a with statement for compatibility with older python versions
    if exists(path) and not overwrite:
      raise AzkabanError('Path %r already exists.' % (path, ))
    if not (len(self._jobs) or len(self._files)):
      raise AzkabanError('Building empty project.')
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
    return getsize(path)

  def main(self, suppress_warning=False):
    """Command line argument parser.

    This method will be removed in a future version (using the `azkaban`
    executable is now the preferred way of running the CLI).

    """
    from sys import argv
    from .__main__ import main
    import warnings
    script = argv[0]
    argv.insert(1, self.name)
    msg = """

      Use azkaban executable instead of running module directly:

        $ azkaban %s%s

      This current method leads to inconsistent error handling and will be
      disabled in a future release.
    """ % (
      ' '.join(argv[1:]),
      '' if argv[2] == 'run' or script == 'jobs.py' else ' -s %s' % (script, ),
    )
    if not suppress_warning:
      warnings.simplefilter('default')
    warnings.warn(msg, DeprecationWarning)
    main(self)

  @classmethod
  def load_from_script(cls, script, name=None):
    """Get project from script.

    :param script: string representing a python module
    :param name: project name

    If `name` is unspecified:
    * if a single project is found by loading `script` that project is returned.
    * in any other case, an error is thrown.

    """
    sys_path.append(dirname(script))
    module = splitext(basename(script.rstrip(sep)))[0]
    try:
      __import__(module)
    except ImportError:
      raise AzkabanError('Unable to import script %r.' % (script, ))
    else:
      if name:
        try:
          return cls._registry[name]
        except KeyError:
          raise AzkabanError(
            'Unable to find project with name %r in script %r.\n'
            'Available projects: %r.'
            % (name, script, ', '.join(cls._registry.keys()))
          )
      else:
        if len(cls._registry) == 1:
          return cls._registry.popitem()[1]
        elif not cls._registry:
          raise AzkabanError('No project found in %r.' % (script, ))
        else:
          raise AzkabanError(
            'Multiple projects found in %r: %s.\n'
            'Disambiguate using the --project option.'
            % (', '.join(cls._registry.keys()), script)
          )
