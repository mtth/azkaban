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
  rcpath = expanduser('~/.azkabanrc')
  if alias:
    parser = RawConfigParser({'user': '', 'session_id': ''})
    parser.read(rcpath)
    if not parser.has_section(alias):
      raise AzkabanError('Missing alias %r.' % (alias, ))
    elif not parser.has_option(alias, 'url'):
      raise AzkabanError('Missing url for alias %r.' % (alias, ))
    else:
      url = parser.get(alias, 'url')
      user = parser.get(alias, 'user')
      session_id = parser.get(alias, 'session_id')
  elif url:
    session_id = None
    parsed_url = url.split('@')
    parsed_url_length = len(parsed_url)
    if parsed_url_length == 1:
      user = getuser()
      url = parsed_url[0]
    elif parsed_url_length == 2:
      user = parsed_url[0]
      url = parsed_url[1]
    else:
      raise AzkabanError('Malformed url: %r' % (url, ))
  else:
    # value error since this is never supposed to happen when called by the
    # CLI (handled by docopt)
    raise ValueError('Either url or alias must be specified.')
  url = url.rstrip('/')
  user = user or getuser()
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
      parser.set(alias, 'session_id', session_id)
      with open(rcpath, 'w') as writer:
        parser.write(writer)
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
