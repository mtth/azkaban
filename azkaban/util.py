#!/usr/bin/env python
# encoding: utf-8

"""Utility module."""


from ConfigParser import (NoOptionError, NoSectionError, ParsingError,
  RawConfigParser)
from contextlib import contextmanager
from functools import wraps
from os import close, remove
from os.path import exists, expanduser
from requests import ConnectionError
from requests.exceptions import MissingSchema
from tempfile import mkstemp
import requests as rq
import sys


class AzkabanError(Exception):

  """Base error class."""

  def __init__(self, message, *args):
    super(AzkabanError, self).__init__(message % args or ())


class Config(object):

  """Configuration class."""

  def __init__(self, path=expanduser('~/.azkabanrc')):
    if not exists(path):
      raise AzkabanError('No configuration file found at %r.', path)
    self.parser = RawConfigParser()
    self.path = path
    try:
      self.parser.read(self.path)
    except ParsingError:
      raise AzkabanError('Invalid configuration file %r.', path)

  @property
  def aliases(self):
    """List of registered alias names."""
    if self.parser.has_section('alias'):
      return self.parser.items('alias')
    else:
      return []

  def resolve_alias(self, alias):
    """Get url and session_id associated with an alias.

    :param alias: alias name

    """
    try:
      url = self.parser.get('alias', alias)
    except (NoOptionError, NoSectionError):
      raise AzkabanError('Alias %r not found.', alias)
    try:
      session_id = self.parser.get('session_id', alias)
    except (NoOptionError, NoSectionError):
      session_id = None
    return {'url': url, 'session_id': session_id}

  def save(self):
    """Save configuration parser back to file."""
    with open(self.path, 'w') as writer:
      self.parser.write(writer)

  def get_default_option(self, command, option):
    """Get default option value for a command.

    :param command: command the option should be looked up for
    :param option: name of the option

    """
    try:
      return self.parser.get(command, 'default.%s' % (option, ))
    except (NoOptionError, NoSectionError):
      raise AzkabanError(
        'No default option %r set for command %r. '
        'Specify one using the --%s flag.',
        option, command, option,
      )

  def docopt(self, *args, **kwargs):
    """Light wrapper around docopt which injects configuration defaults.

    :param *args: forwarded to docopt
    :param **kwargs: forwarded to docopt

    Note that some care must be taken when dealing with exclusive options.

    """
    pass


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

  :param *error_classes: error classes

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
  except AttributeError:
    raise ValueError('Invalid HTTP method: %r.' % (method, ))
  else:
    try:
      response = handler(url, verify=False, **kwargs)
    except ConnectionError:
      raise AzkabanError('Unable to connect to azkaban at %r.' % (url, ))
    except MissingSchema:
      raise AzkabanError('Invalid azkaban server url: %r.' % (url, ))
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
