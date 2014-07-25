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
from mimetypes import guess_type
from os import close, remove
from os.path import exists, expanduser
from requests.packages.urllib3.filepost import choose_boundary
from tempfile import mkstemp
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

  :param files: List of filepaths. For more control, each file can also be
    represented as a dictionary with keys `'path'`, `'name'`, and `'type'`.
  :param params: Optional dictionary of parameters that will be included in the
    form.
  :param callback: Arguments `cur_bytes`, `tot_bytes`, `index`.
  :param chunksize: Size of each streamed file chunk.

  Usage:

  .. code:: python

    from requests import post

    form = MultipartForm(files={'a.txt': 'a.txt'})
    post('http://your.url', data=form, headers=form.headers)

  """

  def __init__(self, files, params=None, callback=None, chunksize=4096):
    self._boundary = choose_boundary()
    self._params = params
    self._files = [
      {'path': o} if isinstance(o, basestring) else o
      for o in files
    ]
    self._callback = callback
    self._chunksize = chunksize
    # generate content type header
    self.headers = {
      'Content-Type': 'multipart/form-data; boundary=%s' % (self._boundary, )
    }
    # prepare files
    self._files = []
    for opts in files:
      if isinstance(opts, basestring):
        opts = {'path': opts}
      opts.setdefault('name', opts.get('name') or osp.basename(opts['path']))
      opts.setdefault('type', opts.get('type') or guess_type(opts['name'])[0])
      self._files.append(opts)

  def __iter__(self):
    def _generator(callback=self._callback):
      """Overall generator. Note the callback caching."""
      # set up counters used in the callback
      cur_bytes = 0
      tot_bytes = self.size
      # start the content body with the form parameters
      if self._params:
        params_content = b''.join(
          b'%s%s' % (self._get_section_header(name), content)
          for name, content in self._params.items()
        )
      else:
        params_content = b''
      yield params_content
      # follow up with the files
      if len(self._files) == 1:
        # simple case, only one file (included as any other form param)
        file_opts = self._files[0]
        yield self._get_section_header(
          name='file',
          filename=file_opts['name'],
          content_type=file_opts['type'],
        )
        for chunk in stream_file(file_opts['path'], self._chunksize):
          cur_bytes += len(chunk)
          yield chunk
          if callback:
            callback(cur_bytes, tot_bytes, 0)
      else:
        # we need to group all files in a single multipart/mixed section
        file_boundary = choose_boundary()
        yield self._get_section_header(
          name='files',
          content_type='multipart/mixed; boundary=%s' % (file_boundary, )
        )
        for index, file_opts in enumerate(self._files):
          yield self._get_section_header(
            filename=file_opts['name'],
            content_type=file_opts['type'],
          )
          for chunk in stream_file(file_opts['path'], self._chunksize):
            cur_bytes += len(chunk)
            yield chunk
            if callback:
              callback(cur_bytes, tot_bytes, index)
        yield b'\r\n--%s--' % (file_boundary, )
      yield b'\r\n--%s--\r\n' % (self._boundary, )
    return _generator()

  @property
  def size(self):
    """Total size of all the files to be streamed.

    Note that this doesn't include the bytes used for the header and
    parameters.

    """
    return sum(osp.getsize(d['path']) for d in self._files)

  def _get_section_header(self, name=None, content_disposition='form-data',
    filename=None, content_type=None, boundary=None):
    """Non streamed. Returns a string."""
    return (
      b'\r\n'
      b'--%(b)s\r\n'
      b'Content-Disposition: %(d)s%(n)s%(f)s\r\n'
      b'%(t)s\r\n'
      % {
        'b': boundary or self._boundary,
        'd': content_disposition,
        'n': b'; name="%s"' % (name, ) if name else b'',
        'f': b'; filename="%s"' % (filename, ) if filename else b'',
        't': b'Content-Type: %s\r\n' % (content_type, ) if content_type else b''
      }
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

def stream_file(path, chunksize):
  """Get iterator over a file's contents.

  :param path: Path to file.
  :param chunksize: Bytes per chunk.

  """
  with open(path, 'rb') as reader:
    while True:
      chunk = reader.read(chunksize)
      if chunk:
        yield chunk
      else:
        break
