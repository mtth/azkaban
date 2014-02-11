#!/usr/bin/env python
# encoding: utf-8

"""Utility module."""


from contextlib import contextmanager
from os import close, remove
from os.path import exists
from requests import ConnectionError
from requests.exceptions import MissingSchema
from sys import stdout
from tempfile import mkstemp

import requests as rq


class AzkabanError(Exception):

  """Base error class."""


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

def tabularize(items, fields, header=True, writer=stdout):
  """Formatted list of dictionaries.

  :param items: list of dictionaries
  :param fields: list of keys
  :param writer: output writer

  Will raise `ValueError` if `items` is empty.

  """
  if not items:
    raise ValueError('empty items')
  widths = [max(len(str(e.get(field, ''))) for e in items) for field in fields]
  widths = [max(w, len(f)) for (w, f) in zip(widths, fields)]
  tpl = '%s\n' % (''.join('%%%ss' % (w + 1, ) for w in widths), )
  if header:
    writer.write(tpl % tuple(fields))
  for item in items:
    writer.write(tpl % tuple(item.get(f, '') for f in fields))
    writer.flush()

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

def azkaban_request(method, url, **kwargs):
  """Make request to azkaban server and catch common errors.

  :param method: get, post, etc.
  :param url: endpoint url
  :param kwargs: arguments forwarded to the request handler

  This function is meant to handle common errors and return a more helpful
  message than the default one.

  """
  try:
    handler = getattr(rq, method.lower())
  except AttributeError as err:
    raise ValueError('invalid method: %r' % (method, ))
  else:
    try:
      response = handler(url, verify=False, **kwargs)
    except ConnectionError:
      raise AzkabanError('unable to connect to azkaban at %r' % (url, ))
    except MissingSchema:
      raise AzkabanError('invalid azkaban server url: %r' % (url, ))
    else:
      return response

def extract_json(response):
  """Extract json from Azkaban response, gracefully handling errors.

  :param response: request response object

  """
  try:
    json = response.json()
  except ValueError:
    # no json decoded probably
    raise ValueError('No JSON decoded from response %r' % (response.text, ))
  else:
    if 'error' in json:
      raise AzkabanError(json['error'])
    elif json.get('status') == 'error':
      raise AzkabanError(json['message'])
    else:
      return json
