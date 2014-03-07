#!/usr/bin/env python
# encoding: utf-8

"""Job definition module."""


from .util import flatten


class Job(object):

  """Base Azkaban job.

  :param options: tuple of dictionaries. The final job options are built from
    this tuple by keeping the latest definition of each option. Furthermore, by
    default, any nested dictionary will be flattened (combining keys with
    `'.'`). Both these features can be changed by simply overriding the job
    constructor.

  To enable more functionality, subclass and override the :meth:`on_add` and
  :meth:`on_build` methods.

  """

  def __init__(self, *options):
    self.options = {}
    for option in options:
      self.options.update(flatten(option))

  def build(self, path):
    """Create job file.

    :param path: path where job file will be created. Any existing file will
      be overwritten.

    """
    with open(path, 'w') as writer:
      for key, value in sorted(self.options.items()):
        writer.write('%s=%s\n' % (key, value))

  def on_add(self, project, name):
    """Handler called when the job is added to a project.

    :param project: :class:`~azkaban.project.Project` instance
    :param name: name corresponding to this job in the project.

    The default implementation does nothing.

    """
    pass

  def on_build(self, project, name):
    """Handler called when a project including this job is built.

    :param project: :class:`~azkaban.project.Project` instance
    :param name: name corresponding to this job in the project.

    The default implementation does nothing.

    """
    pass
