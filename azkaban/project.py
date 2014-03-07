#!/usr/bin/env python
# encoding: utf-8

"""Project definition module."""


from os import sep
from os.path import (abspath, basename, dirname, exists, isabs, isdir, join,
  relpath, splitext)
from traceback import format_exc
from weakref import WeakValueDictionary
from zipfile import ZipFile
from .util import AzkabanError, temppath
import logging
import sys


logger = logging.getLogger(__name__)


class Project(object):

  """Azkaban project.

  :param name: Name of the project.
  :param register: Add project to registry. Setting this to `False` will make
    it invisible to the CLI.
  :param root: Path to a root file or directory used to enable adding files
    using relative paths (typically used with `root=__file__`).

  """

  root = None
  _registry = WeakValueDictionary()

  def __init__(self, name, root=None, register=True):
    self.name = name
    if root:
      self.root = abspath(root if isdir(root) else dirname(root))
    if register:
      self._registry[name] = self
    self._jobs = {}
    self._files = {}

  def __str__(self):
    return self.name

  @property
  def files(self):
    """Returns a list of files that will be included in the project archive.

    This property should not be used to add files. Use :meth:`add_file`
    instead.

    """
    return [relpath(e) for e in self._files]

  @property
  def jobs(self):
    """Returns a dictionary with each job options.

    This property should not be used to add jobs. Use :meth:`add_job` instead.

    """
    return dict(
      (name, job.options)
      for name, job in self._jobs.items()
    )

  def add_file(self, path, archive_path=None):
    """Include a file in the project archive.

    :param path: Path to file.
    :param archive_path: Path to file in archive (defaults to same as `path`).

    If the current project has its `root` parameter specified, this method will
    allow relative paths (and join those with the project's root). Otherwise,
    it will throw an error. This is done to avoid having files in the archive
    with lower level destinations than the base root directory.

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

    :param name: Name assigned to job (must be unique).
    :param job: :class:`~azkaban.job.Job` instance.

    This method triggers the :meth:`~azkaban.job.Job.on_add` method on the
    added job (passing the project and name as arguments). The handler will be
    called right after the job is added.

    """
    logger.debug('adding job %r', name)
    if name in self._jobs:
      raise AzkabanError('Duplicate job name: %r.' % (name, ))
    else:
      self._jobs[name] = job
      job.on_add(self, name)

  def merge_into(self, project, relative=False, unregister=False):
    """Merge one project with another.

    :param project: Target :class:`Project` to merge into.
    :param relative: If set to `True`, files added relative to the current
      project's root will retain their relative paths. The default behavior is
      to always keep the same files when merging (even if the new project's
      root is different).
    :param unregister: Unregister project after merging it.

    The current project remains unchanged while the target project gains all
    the current project's jobs and files. Note that if the `relative` option
    is set to `True`, files can end up having different absolute paths.

    """
    logger.debug('merging into project %r', project.name)
    root = project.root
    if not relative:
      project.root = self.root
    try:
      for name, job in self._jobs.items():
        project.add_job(name, job)
      for path, archive_path in self._files.items():
        project.add_file(path, archive_path)
    finally:
      if not relative:
        project.root = root
    if unregister:
      self._registry.pop(self.name)

  def build(self, path, overwrite=False):
    """Create the project archive.

    :param path: Destination path.
    :param overwrite: Don't throw an error if a file already exists at `path`.

    Triggers the :meth:`~azkaban.job.Job.on_build` method on each job inside the
    project (passing itself and the job's name as two argument). This method
    will be called right before the job file is generated.

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
  def load(cls, path, name=None):
    """Load project from script.

    :param path: Path to python module or package.
    :param name: Project name. If not specified and a single project is found
      while loading the script, that project is returned. In any other case
      (no/multiple projects found), an error is thrown.

    """
    sys.path.insert(0, dirname(path))
    module_name = splitext(basename(path.rstrip(sep)))[0]
    try:
      __import__(module_name)
    except ImportError:
      raise AzkabanError(
        'Unable to import script %r.\n%s' % (path, format_exc())
        )
    else:
      if name:
        try:
          return cls._registry[name]
        except KeyError:
          raise AzkabanError(
            'Unable to find project with name %r in script %r.\n'
            'Available projects: %s.'
            % (name, path, ', '.join(cls._registry))
          )
      else:
        if len(cls._registry) == 1:
          return cls._registry.popitem()[1]
        elif not cls._registry:
          raise AzkabanError('No project found in %r.' % (path, ))
        else:
          raise AzkabanError(
            'Multiple projects found in %r: %s.\n'
            'Disambiguate using --project=%s:project_name.'
            % (path, ', '.join(cls._registry), path)
          )
