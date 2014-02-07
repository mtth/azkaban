#!/usr/bin/env python
# encoding: utf-8

"""Utility module."""


from contextlib import contextmanager
from os import close, remove
from os.path import exists
from sys import stdout
from tempfile import mkstemp


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
