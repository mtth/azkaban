#!/usr/bin/env python
# encoding: utf-8

"""Job definition module."""

from .util import flatten, write_properties


class Job(object):

  """Base Azkaban job.

  :param options: tuple of dictionaries. The final job options are built from
    this tuple by keeping the latest definition of each option. Furthermore, by
    default, any nested dictionary will be flattened (combining keys with
    `'.'`). Both these features can be changed by simply overriding the job
    constructor.

  To enable more functionality, subclass and override the :meth:`on_add` and
  :meth:`build` methods. The :meth:`join_option` and :meth:`join_prefix`
  methods are also provided as helpers to write custom jobs.

  """

  def __init__(self, *options):
    self.options = {}
    for option in options:
      self.options.update(flatten(option))

  def build(self, path=None, header=None):
    """Write job file.

    :param path: Path where job file will be created. Any existing file will
      be overwritten. Writes to stdout if no path is specified.
    :param header: Optional comment to be included at the top of the job file.

    """
    write_properties(self.options, path=path, header=header)

  def on_add(self, project, name, **kwargs):
    """Handler called when the job is added to a project.

    :param project: :class:`~azkaban.project.Project` instance
    :param name: name corresponding to this job in the project.
    :param kwargs: Keyword arguments. If this method is triggered by
      :meth:`~azkaban.project.Project.add_job`, the latter's keyword arguments
      will simply be forwarded. Else if this method is triggered by a merge,
      kwargs will be a dictionary with single key `'merging'` and value the
      merged project.

    The default implementation does nothing.

    """
    pass

  def join_option(self, option, sep, formatter='%s'):
    """Helper method to join iterable options into a string.

    :param key: Option key. If the option doesn't exist, this method does
      nothing.
    :param sep: Separator used to concatenate the string.
    :param formatter: Pattern used to format the option values.

    Example usage:

    .. code-block:: python

      class MyJob(Job):

        def __init__(self, *options):
          super(MyJob, self).__init__(*options)
          self.join_option('dependencies', ',')

      # we can now use lists to define job dependencies
      job = MyJob({'type': 'noop', 'dependencies': ['bar', 'foo']})

    """
    values = self.options.get(option, None)
    if values:
      self.options[option] = sep.join(formatter % (v, ) for v in values)

  def join_prefix(self, prefix, sep, formatter):
    """Helper method to join options starting with a prefix into a string.

    :param prefix: Option prefix.
    :param sep: Separator used to concatenate the string.
    :param formatter: String formatter. It is formatted using the tuple
      `(suffix, value)` where `suffix` is the part of `key` after `prefix`.

    Example usage:

    .. code-block:: python

      class MyJob(Job):

        def __init__(self, *options):
          super(MyJob, self).__init__(*options)
          self.join_prefix('jvm.args', ' ', '-D%s=%s')

      # we can now define JVM args using nested dictionaries
      job = MyJob({'type': 'java', 'jvm.args': {'foo': 48, 'bar': 23}})

    """
    prefix = prefix.rstrip('.')
    opts = []
    for key in list(self.options):
      # copying keys to a list to allow modifying dict in loop
      # note that we aren't using `.keys()` for compatibility with python3
      if key.startswith(prefix):
        opts.append(
          (key.replace('%s.' % (prefix, ), ''), self.options.pop(key))
        )
    if opts:
      self.options[prefix] = sep.join(formatter % a for a in sorted(opts))
