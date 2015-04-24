#!/usr/bin/env python
# encoding: utf-8

"""Extension providing convenience functions for working with subflows.

Embedded flow are very convenient to avoid job duplication (e.g. when running
different variants of a workflow with different set of parameters). The classes
below provide a concise way to leverage flow job options (propagated to all
jobs inside the target subflow) to generate dynamic options.

"""

from ..job import Job
from ..util import flatten


class FlowProperty(object):

  """Dynamic property.

  These job properties are dynamically populated depending on which subflow
  the job is embedded in.

  Note that this class (similarly to the `Project` class) is not thread-safe
  and should only be instantiated from a single thread.

  """

  __register = {}

  def __init__(self, **kwargs):
    self.key = 'flow.property.%s' % (len(self.__register), )
    self.__register[self.key] = kwargs

  def __str__(self):
    return '${%s}' % (self.key, )

  @classmethod
  def get_options(cls, mode):
    """Retrieve all properties for a given mode.

    :param mode: Which dynamic properties to select.

    This should only be called once all dynamic properties have been added (at
    least for the input mode).

    """
    return dict(
      (key, properties[mode])
      for key, properties in cls.__register.items()
      if mode in properties
    )

  @classmethod
  def reset(cls):
    """Reset all properties.

    Mostly only used by tests.

    """
    cls.__register = {}


class FlowJob(Job):

  """Job class for embedded flows.

  :param subflow: Subflow name. Will take precedence over any `'flow.name'`
    option specified in `options`.
  :param mode: Used for dynamic `FlowProperty`'s.
  :param \*options: Regular `Job` options.

  """

  def __init__(self, subflow, mode, *options):
    super(FlowJob, self).__init__(*options)
    self.mode = mode
    self.options['type'] = 'flow'
    self.options['flow.name'] = subflow

  def build(self, *args, **kwargs):
    """Build job options.

    :param \*args: Positional arguments passed to the base `build` function.
    :param \*\*kwargs: Keyword arguments passed to the base `build` function.

    We delay the full expansion of options until now to make sure that all
    dynamic properties have already been added. Note also that each `FlowJob`
    will contain the list of all dynamic properties defined for its mode.

    """
    self.options.update(FlowProperty.get_options(self.mode))
    super(FlowJob, self).build(*args, **kwargs)
