#!/usr/bin/env python
# encoding: utf-8

"""Utility module."""


from ConfigParser import RawConfigParser
from contextlib import contextmanager
from getpass import getpass, getuser
from os import close, remove
from os.path import exists, expanduser
from requests import ConnectionError
from requests.exceptions import MissingSchema
from tempfile import mkstemp

import requests as rq


class AzkabanError(Exception):

  """Base error class."""


class Config(object):

  """Configuration class. Not meant to be instantiated."""

  rcpath = expanduser('~/.azkabanrc')

  def __init__(self):
    self.parser = RawConfigParser()
    if exists(self.rcpath):
      self.parser.read(self.rcpath)

  def get(self, section, name):
    """Get option from the azkaban RC file.

    :param section: which section to look into
    :param name: option name

    Sugar method that wraps the `RawConfigParser.get` method to raise an
    `AzkabanError` if no section of name is found in the configuration file.

    """
    if not self.parser.has_section(section):
      raise AzkabanError('Missing section %r.' % (section, ))
    elif not self.parser.has_option(section, name):
      raise AzkabanError('Missing option %r for section %r.' % (name, section))
    else:
      return self.parser.get(section, name)

  def get_default(self, command, option):
    """Get default option value for a command.

    :param command: command the option should be looked up for
    :param option: name of the option

    """
    try:
      return self.get(command, 'default.%s' % (option, ))
    except AzkabanError:
      raise AzkabanError(
        'No default option %r set for command %r. '
        'Specify one using the --%s flag.'
        % (option, command, option)
      )

  def set(self, section, name, value):
    """Proxy method to set an option on the parser.

    :param section: section name
    :param name: option name
    :param value: value to set

    """
    self.parser.set(section, name, value)

  def save(self):
    """Save configuration parser back to file."""
    with open(self.rcpath, 'w') as writer:
      self.parser.write(writer)


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
  except AttributeError:
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

def get_session(url=None, password=None, alias=None):
  """Get URL and associated valid session ID.

  :param url: http endpoint (including port and optional user)
  :param password: password used to log into Azkaban (only used if no alias
    is provided)
  :param alias: alias name used to find the URL, user, and an existing
    session ID if possible (will override the `url` parameter)

  """
  config = Config()
  if url:
    session_id = None
  else:
    alias = alias or config.get_default('azkaban', 'alias')
    url = config.get('alias', alias)
    try:
      session_id = config.get('session_id', alias)
    except AzkabanError:
      session_id = None
  parsed_url = url.rstrip('/').split('@')
  parsed_url_length = len(parsed_url)
  if parsed_url_length == 1:
    user = getuser()
    url = parsed_url[0]
  elif parsed_url_length == 2:
    user = parsed_url[0]
    url = parsed_url[1]
  else:
    raise AzkabanError('Malformed url: %r' % (url, ))
  if not session_id or azkaban_request(
    'POST',
    '%s/manager' % (url, ),
    data={'session.id': session_id},
  ).text:
    password = password or getpass('azkaban password for %s: ' % (user, ))
    res = extract_json(azkaban_request(
      'POST',
      url,
      data={'action': 'login', 'username': user, 'password': password},
    ))
    session_id = res['session.id']
    if alias:
      config.set('session_id', alias, session_id)
      config.save()
  return {'url': url, 'session_id': session_id}

def get_execution_status(exec_id, url, session_id):
  """Get status of an execution.

  :param exec_id: execution ID
  :param url: Azkaban server endpoint
  :param session_id: valid session id

  """
  return extract_json(azkaban_request(
    'GET',
    '%s/executor' % (url, ),
    params={
      'execid': exec_id,
      'ajax': 'fetchexecflow',
    },
    cookies={
      'azkaban.browser.session.id': session_id,
    },
  ))

def get_job_logs(exec_id, url, session_id, job, offset=0, limit=50000):
  """Get logs from a job execution.

  :param exec_id: execution ID
  :param url: Azkaban server endpoint
  :param session_id: valid session id
  :param job: job name
  :param offset: log offset
  :param limit: size of log to download

  """
  return extract_json(azkaban_request(
    'GET',
    '%s/executor' % (url, ),
    params={
      'execid': exec_id,
      'jobId': job,
      'ajax': 'fetchExecJobLogs',
      'offset': offset,
      'length': limit,
    },
    cookies={
      'azkaban.browser.session.id': session_id,
    },
  ))

def cancel_execution(exec_id, url, session_id):
  """Cancel workflow execution.

  :param exec_id: execution ID
  :param url: Azkaban server endpoint
  :param session_id: valid session id

  """
  res = extract_json(azkaban_request(
    'GET',
    '%s/executor' % (url, ),
    params={
      'execid': exec_id,
      'ajax': 'cancelFlow',
    },
    cookies={
      'azkaban.browser.session.id': session_id,
    },
  ))
  if 'error' in res:
    raise AzkabanError('Execution %s is not running.' % (exec_id, ))
