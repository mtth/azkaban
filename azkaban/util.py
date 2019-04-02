#!/usr/bin/env python
# encoding: utf-8

"""Utility module."""

from contextlib import contextmanager
from functools import wraps
from itertools import chain
from logging.handlers import TimedRotatingFileHandler
from mimetypes import guess_type
from os import close, remove
from os.path import exists, expanduser
from requests.packages.urllib3 import disable_warnings
from requests.packages.urllib3.filepost import choose_boundary
from six import b, string_types
from six.moves.configparser import (NoOptionError, NoSectionError,
  ParsingError, RawConfigParser)
from tempfile import gettempdir, mkstemp
from traceback import print_exc
import logging as lg
import os.path as osp
import re
import sys
import warnings as wr


_logger = lg.getLogger(__name__)


class AzkabanError(Exception):

  """Base error class."""

  def __init__(self, message, *args):
    message = message % args if args else message
    super(AzkabanError, self).__init__(message)
    self.message = message


class Adapter(lg.LoggerAdapter):

  """Logger adapter that includes a prefix to all messages.

  :param prefix: Prefix string.
  :param logger: Logger instance where messages will be logged.
  :param extra: Dictionary of contextual information, passed to the formatter.

  """

  def __init__(self, prefix, logger, extra=None):
    lg.LoggerAdapter.__init__(self, logger, extra)
    # not using super since `LoggerAdapter` is an old-style class in python 2.6
    self.prefix = prefix

  def process(self, msg, kwargs):
    """Adds a prefix to each message.

    :param msg: Original message.
    :param kwargs: Keyword arguments that will be forwarded to the formatter.

    """
    return '%s :: %s' % (self.prefix, msg), kwargs


class Config(object):

  """Configuration class.

  :param path: path to configuration file. If no file exists at that location,
    the configuration parser will be empty. Defaults to `~/.azkabanrc`.

  """

  def __init__(self, path=None):
    self.parser = RawConfigParser()
    self.path = path or expanduser('~/.azkabanrc')
    # TODO: make the default path be configurable via an environment variable.
    if exists(self.path):
      try:
        self.parser.read(self.path)
      except ParsingError:
        raise AzkabanError('Invalid configuration file %r.', self.path)
      else:
        # TODO: remove this in 1.0.
        if self._convert_aliases():
          self.save()
          self.parser.read(self.path)


  def save(self):
    """Save configuration parser back to file."""
    with open(self.path, 'w') as writer:
      self.parser.write(writer)

  def get_option(self, command, name, default=None):
    """Get option value for a command.

    :param command: Command the option should be looked up for.
    :param name: Name of the option.
    :param default: Default value to be returned if not found in the
      configuration file. If not provided, will raise
      :class:`~azkaban.util.AzkabanError`.

    """
    try:
      return self.parser.get(command, name)
    except (NoOptionError, NoSectionError):
      if default is not None:
        return default
      else:
        raise AzkabanError(
          'No %(name)s found in %(path)r for %(command)s.\n'
          'You can specify one by adding a `%(name)s` option in the '
          '`%(command)s` section.'
          % {'command': command, 'name': name, 'path': self.path}
        )

  def get_file_handler(self, command):
    """Add and configure file handler.

    :param command: Command the options should be looked up for.

    The default path can be configured via the `default.log` option in the
    command's corresponding section.

    """
    handler_path = osp.join(gettempdir(), '%s.log' % (command, ))
    try:
      handler = TimedRotatingFileHandler(
        self.get_option(command, 'default.log', handler_path),
        when='midnight', # daily backups
        backupCount=1,
        encoding='utf-8',
      )
    except IOError:
      wr.warn('Unable to write to log file at %s.' % (handler_path, ))
    else:
      handler_format = '[%(levelname)s] %(asctime)s :: %(name)s :: %(message)s'
      handler.setFormatter(lg.Formatter(handler_format))
      return handler

  def _convert_aliases(self):
    """Convert old-style aliases to new-style."""
    parser = self.parser
    if not parser.has_section('alias'):
      return False # Nothing to do.
    for alias, url in parser.items('alias'):
      section = 'alias.%s' % (alias, )
      if not parser.has_section(section):
        # Only update if the alias doesn't yet.
        parser.add_section(section)
        parser.set(section, 'url', url)
        parser.set(section, 'verify', 'false') # Backwards compatibility.
    parser.remove_section('alias')
    return True


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

    form = MultipartForm(files=['README.md'])
    post('http://your.url', headers=form.headers, data=form)

  """

  def __init__(self, files, params=None, callback=None, chunksize=4096):
    self._boundary = choose_boundary()
    self._params = params
    self._callback = callback
    self._chunksize = chunksize
    # generate content type header
    self.headers = {
      'Content-Type': 'multipart/form-data; boundary=%s' % (self._boundary, )
    }
    # prepare files
    self._files = []
    for opts in files:
      if isinstance(opts, string_types):
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
        params_content = b(''.join(
          '%s%s' % (self._get_section_header(name), content)
          for name, content in self._params.items()
        ))
      else:
        params_content = b''
      yield params_content
      # follow up with the files
      if len(self._files) == 1:
        # simple case, only one file (included as any other form param)
        file_opts = self._files[0]
        yield b(self._get_section_header(
          name='file',
          filename=file_opts['name'],
          content_type=file_opts['type'],
        ))
        for chunk in stream_file(file_opts['path'], self._chunksize):
          cur_bytes += len(chunk)
          yield chunk
          if callback:
            callback(cur_bytes, tot_bytes, 0)
      else:
        # we need to group all files in a single multipart/mixed section
        file_boundary = choose_boundary()
        yield b(self._get_section_header(
          name='files',
          content_type='multipart/mixed; boundary=%s' % (file_boundary, )
        ))
        for index, file_opts in enumerate(self._files):
          yield b(self._get_section_header(
            filename=file_opts['name'],
            content_type=file_opts['type'],
          ))
          for chunk in stream_file(file_opts['path'], self._chunksize):
            cur_bytes += len(chunk)
            yield chunk
            if callback:
              callback(cur_bytes, tot_bytes, index)
        yield b('\r\n--%s--' % (file_boundary, ))
      yield b('\r\n--%s--\r\n' % (self._boundary, ))
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
      '\r\n'
      '--%(b)s\r\n'
      'Content-Disposition: %(d)s%(n)s%(f)s\r\n'
      '%(t)s\r\n'
      % {
        'b': boundary or self._boundary,
        'd': content_disposition,
        'n': '; name="%s"' % (name, ) if name else '',
        'f': '; filename="%s"' % (filename, ) if filename else '',
        't': 'Content-Type: %s\r\n' % (content_type, ) if content_type else ''
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

  :param error_classes: Error classes.
  :param log: Filepath to log file.

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
        _logger.error(err)
        sys.stderr.write('%s\n' % (err, ))
        sys.exit(1)
      except Exception: # catch all
        _logger.exception('Unexpected exception.')
        print_exc()
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

def read_properties(*paths):
  """Read options from a properties file and return them as a dictionary.

  :param \*paths: Paths to properties file. In the case of multiple definitions
    of the same option, the latest takes precedence.

  Note that not all features of `.properties` files are guaranteed to be
  supported.

  """
  comment_p = re.compile(r'\s*(?:#|!)')
  continuation_whitespace_p = re.compile(r'\\\n\s*')
  separator_p = re.compile(r'(?<!\\)\s*(?::|=|\s)\s*')
  separator_replacement_p = re.compile(r'\\(:|=|\s)')
  opts = {}
  for path in paths:
    if not osp.exists(path):
      raise AzkabanError('No properties file found at %s.', path)
    try:
      with open(path) as reader:
        contents = continuation_whitespace_p.sub('', reader.read())
        lines = (
          tuple(s.strip() for s in separator_p.split(line, 1))
          for line in contents.split('\n')
          if line.strip() and not comment_p.match(line)
        )
        opts.update(dict(
          (
            separator_replacement_p.sub(lambda m: m.group(1), t[0]),
            t[1] if len(t) == 2 else ''
          )
          for t in lines
        ))
    except Exception:
      raise AzkabanError('Unsupported properties file: %r', path)
  return opts

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

def suppress_urllib_warnings():
  """Capture urllib warnings if possible, else disable them (python 2.6)."""
  try:
    lg.captureWarnings(True)
  except AttributeError:
    disable_warnings()
