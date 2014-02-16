#!/usr/bin/env python
# encoding: utf-8

"""Job definition module."""


from .util import AzkabanError, flatten


class Job(object):

  """Base Azkaban job.

  :param options: tuple of dictionaries. The final job options are built from
    this tuple by keeping the latest definition of each option. This can be
    changed by overriding the `option` property. Furthermore, by default, any
    nested dictionary will be flattened (combining keys with '.'). Set the
    class attribute `flatten` to `False` to disable this behavior.

  To enable more functionality, subclass and override the `on_add` and
  `on_build` methods.

  """

  flatten = True

  def __init__(self, *options):
    self.options_tuple = options

  @property
  def option_names(self):
    """Set of all option keys."""
    if self.options_tuple:
      if self.flatten:
        names = set.union(*[set(flatten(opts)) for opts in self.options_tuple])
      else:
        names = set.union(*[set(opts) for opts in self.options_tuple])
    else:
      names = set()
    return names

  def get_option(self, name, earliest=False):
    """Get latest definition of an option.

    :param name: name of an option
    :param earliest: return first definition of an option instead of latest

    Raises an error if the option isn't present.

    """
    options_index = 0
    if self.flatten:
      _options_tuple = [flatten(opts) for opts in self.options_tuple]
    else:
      _options_tuple = self.options_tuple
    if not earliest:
      _options_tuple = _options_tuple[::-1]
    try:
      while name not in _options_tuple[options_index]:
        options_index += 1
    except IndexError:
      raise AzkabanError('option %r not found' % (name, ))
    else:
      return _options_tuple[options_index][name]

  def get_option_list(self, name):
    """Get list of all definitions of an option (from latest to earliest).

    :param name: name of an option

    Returns an empty list if the option isn't defined anywhere.

    """
    option_list = []
    for options in self.options_tuple:
      flattened_options = flatten(options)
      if name in flattened_options:
        option_list.append(flattened_options[name])
    return option_list

  @property
  def options(self):
    """Combined job options.

    The default implementation takes the latest definition of each option.
    Override this to implement custom logic for special option names. E.g. to
    join lists of options.

    """
    return dict((name, self.get_option(name)) for name in self.option_names)

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
