#!/usr/bin/env python
# encoding: utf-8

"""Project definition module."""


from os import sep
from os.path import (basename, dirname, exists, isabs, isdir, join, relpath, 
  splitext)
from weakref import WeakValueDictionary
from zipfile import ZipFile
from .util import AzkabanError, temppath
import logging
import sys


logger = logging.getLogger(__name__)


class Project(object):

  """Azkaban project.

  :param name: name of the project
  :param register: add project to registry. setting this to false will make it
    invisible to the CLI
  :param root: optional path to a root file or directory used to enable adding
    files with relative paths (typically used with root=__file__)

  """

  _registry = WeakValueDictionary()

  def __init__(self, name, root=None, register=True):
    self.name = name
    self.root = root if not root or isdir(root) else dirname(root)
    if register:
      self._registry[name] = self
    self._jobs = {}
    self._files = {}

  def __str__(self):
    return self.name

  @property
  def files(self):
    """Returns a list of files that will be included in the project archive.

    This property should not be used to add files. Use `add_files` instead.

    """
    return [relpath(e) for e in self._files]

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

  @classmethod
  def load_from_script(cls, script, name=None):
    """Get project from script.

    :param script: string representing a python module
    :param name: project name

    If `name` is unspecified:
    * if a single project is found by loading `script` that project is returned.
    * in any other case, an error is thrown.

    """
    sys.path.append(dirname(script))
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
            'Disambiguate using --project=%s:project_name.'
            % (', '.join(cls._registry.keys()), script, script)
          )
