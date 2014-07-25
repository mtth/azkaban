#!/usr/bin/env python
# encoding: utf-8

"""Utility module."""

from __future__ import unicode_literals

try:
  from ConfigParser import (NoOptionError, NoSectionError, ParsingError,
    RawConfigParser)
except ImportError:
  # python 3
  from configparser import (NoOptionError, NoSectionError, ParsingError,
    RawConfigParser)

from contextlib import contextmanager
from functools import wraps
from io import BytesIO
from itertools import chain
from mimetools import choose_boundary
from mimetypes import guess_type
from os import close, remove
from os.path import exists, expanduser
from requests.packages.urllib3.fields import guess_content_type
from tempfile import mkstemp
from uuid import uuid4
import codecs
import os.path as osp
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


class MultipartForm(object):

  """Form allowing streaming.

  Usage:

  .. code:: python

    from requests import post

    form = MultipartForm(files={'a.txt': 'a.txt'})
    post('http://your.url', data=form, headers=form.headers)

  """

  # headers to be formatted using the boundary (different for each)

  def __init__(self, files, data=None, callback=None):
    self.outer_boundary = self._get_boundary()
    self.inner_boundary = self._get_boundary()
    self.headers = {
      'Content-Type': 'multipart/form-data; boundary=%s' % (self.outer_boundary, )
    }
    if data:
      data_content = ''.join(
        self._get_data_part(name, content, self.outer_boundary)
        for name, content in data.items()
      )
    else:
      data_content = ''
    # self.static_content = (
    #   b'%s'
    #   b'\r\n--%s\r\n'
    #   b'Content-Disposition: form-data; name="files"\r\n'
    #   b'Content-Type: multipart/mixed; boundary=%s\r\n'
    # ) % (data_content, self.outer_boundary, self.inner_boundary)
    self.static_content = data_content
    self.files = files

  def _get_boundary(self):
    return uuid4().hex

  def _get_data_part(self, name, content, boundary):
    """Non streamed. Returns a string."""
    # TODO: ensure name and content are bytes
    header = (
      b'\r\n--%s\r\n'
      b'Content-Disposition: form-data; name="%s"\r\n'
      b'\r\n'
      % (boundary, name)
    )
    return header + str(content)

  def _get_file_part(self, boundary, path, name=None, content_type=None, chunksize=4096):
    """Streamed. Returns a generator."""
    # TODO: ensure name and path are bytes
    name = name or osp.basename(path)
    content_type = content_type or guess_type(name)[0] or 'application/octet-stream'
    header = (
      b'\r\n--%s\r\n'
      # b'Content-Disposition: file; filename="%s"\r\n'
      b'Content-Disposition: form-data; name="file"; filename="%s"\r\n'
      b'Content-Type: %s\r\n'
      b'\r\n'
      % (boundary, name, content_type)
    )
    def _generator():
      """Data generator."""
      yield header
      with open(path, 'rb') as reader:
        while True:
          chunk = reader.read(chunksize)
          if chunk:
            yield chunk
          else:
            break
    return _generator()

  def __iter__(self):
    def _generator():
      """Overall generator."""
      yield self.static_content
      for file_opts in self.files:
        if isinstance(file_opts, basestring):
          file_opts = {'path': file_opts}
        for chunk in self._get_file_part(
          # boundary=self.inner_boundary,
          boundary=self.outer_boundary,
          path=file_opts['path'],
          name=file_opts.get('name'),
          content_type=file_opts.get('type'),
        ):
          yield chunk
      # yield b'\r\n--%s--\r\n--%s--\r\n' % (self.inner_boundary, self.outer_boundary)
      yield b'\r\n--%s--\r\n' % (self.outer_boundary, )
    return _generator()

  @property
  def content(self):
    """TODO: content docstring."""
    return b''.join(chunk for chunk in self)


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
