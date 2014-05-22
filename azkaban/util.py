#!/usr/bin/env python
# encoding: utf-8

"""Utility module."""


try:
  from ConfigParser import (NoOptionError, NoSectionError, ParsingError,
    RawConfigParser)
except ImportError:
  # python 3
  from configparser import (NoOptionError, NoSectionError, ParsingError,
    RawConfigParser)

from contextlib import contextmanager
from functools import wraps
from itertools import chain
from os import close, remove
from os.path import exists, expanduser
from tempfile import mkstemp
import sys


class AzkabanError(Exception):

  """Base error class."""

  def __init__(self, message, *args):
    super(AzkabanError, self).__init__(message % args if args else message)


class Config(object):

  """Configuration class.

  :param path: path to configuration file. If no file exists at that location,
    the configuration parser will be empty.

  """

  def __init__(self, path=expanduser('~/.azkabanrc')):
    self.parser = RawConfigParser()
    self.path = path
    if exists(path):
      try:
        self.parser.read(self.path)
      except ParsingError:
        raise AzkabanError('Invalid configuration file %r.', path)

  def save(self):
    """Save configuration parser back to file."""
    with open(self.path, 'w') as writer:
      self.parser.write(writer)

  def get_option(self, command, name, default=None):
    """Get default option value for a command.

    :param command: Command the option should be looked up for.
    :param name: Name of the option.
    :param default: Default value to be returned if not found in the
      configuration file. If not provided, will raise
      :class:`~azkaban.util.AzkabanError`.

    """
    try:
      return self.parser.get(command, 'default.%s' % (name, ))
    except (NoOptionError, NoSectionError):
      if default:
        return default
      else:
        raise AzkabanError(
          'No default %(name)s found in %(path)r for %(command)s.\n'
          'You can specify one by adding a `default.%(name)s` option in the '
          '`%(command)s` section.'
          % {'command': command, 'name': name, 'path': self.path}
        )


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

def catch(*error_classes):
  """Returns a decorator that catches errors and prints messages to stderr.

  :param *error_classes: Error classes.

  Also exits with status 1 if any errors are caught.

  """
  def decorator(func):
    """Decorator."""
    @wraps(func)
    def wrapper(*args, **kwargs):
      """Wrapper. Finally."""
      try:
        return func(*args, **kwargs)
      except error_classes as err:
        sys.stderr.write('%s\n' % (err, ))
        sys.exit(1)
    return wrapper
  return decorator

def flatten(dct, sep='.'):
  """Flatten a nested dictionary.

  :param dct: Dictionary to flatten.
  :param sep: Separator used when concatenating keys.

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

  :param size: Size in bytes.

  """
  for suffix in ['bytes', 'kB', 'MB', 'GB', 'TB']:
    if size < 1024.0:
      return '%3.1f%s' % (size, suffix)
    size /= 1024.0

def write_properties(options, path=None, header=None):
  """Write options to properties file.

  :param options: Dictionary of options.
  :param path: Path to file. Any existing file will be overwritten. Writes to
    stdout if no path is specified.
  :param header: Optional comment to be included at the top of the file.

  """
  lines = ('%s=%s\n' % t for t in sorted(options.items()))
  if header:
    lines = chain(['# %s\n' % (header, )], lines)
  if path:
    with open(path, 'w') as writer:
      for line in lines:
        writer.write(line)
  else:
    for line in lines:
      sys.stdout.write(line)
