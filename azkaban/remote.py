#!/usr/bin/env python
# encoding: utf-8

"""Azkaban remote interaction module.

This contains the `Session` class which will be used for all interactions with
a remote Azkaban server.

"""


try:
  from ConfigParser import NoOptionError, NoSectionError
except ImportError:
  # python 3
  from configparser import NoOptionError, NoSectionError

from getpass import getpass, getuser
from os.path import exists
from time import sleep
from .util import AzkabanError, Config, flatten
import logging
import requests as rq


logger = logging.getLogger(__name__)

def _azkaban_request(method, url, **kwargs):
  """Make request to azkaban server and catch common errors.

  :param method: GET, POST, etc.
  :param url: Endpoint url.
  :param **kwargs: Arguments forwarded to the request handler.

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
    except rq.ConnectionError:
      raise AzkabanError('Unable to connect to azkaban at %r.' % (url, ))
    except rq.exceptions.MissingSchema:
      raise AzkabanError('Invalid azkaban server url: %r.' % (url, ))
    else:
      return response

def _extract_json(response):
  """Extract JSON from Azkaban response, gracefully handling errors.

  :param response: Request response object.

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

def _parse_url(url):
  """Parse url.

  :param url: HTTP endpoint (including protocol, port and optional user).

  """
  parsed_url = url.rstrip('/').split('@')
  if len(parsed_url) == 1:
    user = getuser()
    url = parsed_url[0]
  elif len(parsed_url) == 2:
    user = parsed_url[0]
    url = parsed_url[1]
  else:
    raise AzkabanError('Malformed url: %r' % (url, ))
  return (user, url)

def _resolve_alias(config, alias):
  """Get url associated with an alias.

  :param alias: Alias name.

  """
  try:
    return config.parser.get('alias', alias)
  except (NoOptionError, NoSectionError):
    raise AzkabanError('Alias %r not found.', alias)

def _get_session_id(config, url):
  """Retrieve session id associated with url.

  :param url: HTTP endpoint (including protocol, port and optional user).

  """
  try:
    return config.parser.get('session_id', url)
  except NoSectionError:
    config.parser.add_section('session_id')
  except NoOptionError:
    pass


class Session(object):

  """Azkaban session.

  :param url: HTTP endpoint (including protocol, port and optional user).
  :param alias: Alias name.

  This class contains mostly low-level methods that translate directly into
  Azkaban API calls. The :class:`~azkaban.remote.Execution` class should be
  preferred for interacting with workflow executions.

  """

  def __init__(self, url=None, alias=None):
    self.config = Config()
    if not url:
      alias = alias or self.config.get_option('azkaban', 'alias')
      url = _resolve_alias(self.config, alias)
    self.user, self.url = _parse_url(url)
    self.id = _get_session_id(self.config, str(self).replace(':', '.'))
    logger.debug('session %s instantiated', self)

  def __str__(self):
    return '%s@%s' % (self.user, self.url)

  def _refresh(self, attempts, password=None):
    """Refresh session ID.

    :param attempts: Maximum number of attempts to refresh session.
    :param password: Password used to log into Azkaban. If not specified,
      will prompt for one.

    Also caches the session ID for future use.

    """
    logger.debug('refreshing session')
    while True:
      password = password or getpass('Azkaban password for %s: ' % (self, ))
      try:
        res = _extract_json(_azkaban_request(
          'POST',
          self.url,
          data={
            'action': 'login',
            'username': self.user,
            'password': password,
          },
        ))
      except AzkabanError as err:
        if not 'Incorrect Login.' in err.message:
          raise err
        attempts -= 1
        password = None
        if attempts <= 0:
          raise AzkabanError('Too many unsuccessful login attempts. Aborting.')
      else:
        break
    self.id = res['session.id']
    logger.debug('saving session id')
    self.config.parser.set('session_id', str(self).replace(':', '.'), self.id)
    self.config.save()

  def _request(self, method, endpoint, use_cookies=True, attempts=3,
    check_first=False, **kwargs):
    """Make a request to Azkaban using this session.

    :param method: HTTP method.
    :param endpoint: Server endpoint (e.g. manager).
    :param attempts: If current session ID is invalid, maximum number of
      attempts to refresh it.
    :param use_cookies: Include `session_id` in cookies instead of request
      data.
    :param check_first: Send an extra request to check that the current session
      is valid before sending the actual one. This is useful when the request
      is large (e.g. when uploading a project archive).
    :param kwargs: Keyword arguments passed to :func:`_azkaban_request`.

    If the session expired, will prompt for a password to refresh.

    """
    full_url = '%s/%s' % (self.url, endpoint.lstrip('/'))
    logger.debug('sending request to %r: %r', full_url, kwargs)
    for retry in [False, True]:
      if use_cookies:
        kwargs.setdefault('cookies', {})['azkaban.browser.session.id'] = self.id
      else:
        kwargs.setdefault('data', {})['session.id'] = self.id
      if check_first and not retry:
        # this request will return a 200 empty response if the current session
        # ID is valid and a 500 response otherwise
        res = _azkaban_request(
          'POST',
          '%s/manager' % (self.url, ),
          data={'session.id': self.id},
        )
      else:
        res = _azkaban_request(method, full_url, **kwargs) if self.id else None
      if (
        res is None or # explicit check because 500 responses evaluate to False
        '<!-- /.login -->' in res.text or # usual non API error response
        'Login error' in res.text or # special case for API
        '"error" : "session"' in res.text # error when running a flow's jobs
      ):
        logger.debug('request failed because of invalid login')
        self._refresh(attempts)
      elif retry or not check_first:
        return res

  def get_execution_status(self, exec_id):
    """Get status of an execution.

    :param exec_id: Execution ID.

    """
    return _extract_json(self._request(
      method='GET',
      endpoint='executor',
      params={
        'execid': exec_id,
        'ajax': 'fetchexecflow',
      },
    ))

  def get_execution_logs(self, exec_id, offset=0, limit=50000):
    """Get execution logs.

    :param exec_id: Execution ID.
    :param offset: Log offset.
    :param limit: Size of log to download.

    """
    return _extract_json(self._request(
      method='GET',
      endpoint='executor',
      params={
        'execid': exec_id,
        'ajax': 'fetchExecFlowLogs',
        'offset': offset,
        'length': limit,
      },
    ))

  def get_job_logs(self, exec_id, job, offset=0, limit=50000):
    """Get logs from a job execution.

    :param exec_id: Execution ID.
    :param job: Job name.
    :param offset: Log offset.
    :param limit: Size of log to download.

    """
    return _extract_json(self._request(
      method='GET',
      endpoint='executor',
      params={
        'execid': exec_id,
        'jobId': job,
        'ajax': 'fetchExecJobLogs',
        'offset': offset,
        'length': limit,
      },
    ))

  def cancel_execution(self, exec_id):
    """Cancel workflow execution.

    :param exec_id: Execution ID.

    """
    res = _extract_json(self._request(
      method='GET',
      endpoint='executor',
      params={
        'execid': exec_id,
        'ajax': 'cancelFlow',
      },
    ))
    if 'error' in res:
      raise AzkabanError('Execution %s is not running.' % (exec_id, ))
    return res

  def create_project(self, name, description):
    """Create project.

    :param name: Project name.
    :param description: Project description.

    """
    return _extract_json(self._request(
      method='POST',
      endpoint='manager',
      data={
        'action': 'create',
        'name': name,
        'description': description,
      },
    ))

  def delete_project(self, name):
    """Delete a project on Azkaban.

    :param name: Project name.

    """
    res = self._request(
      method='GET',
      endpoint='manager',
      params={
        'project': name,
        'delete': 'true',
      },
    )
    msg = "Project '%s' was successfully deleted" % (name, )
    if not msg in res.text:
      raise AzkabanError('Delete failed. Check permissions and existence.')
    return res

  def run_workflow(self, name, flow, jobs=None, concurrent=True,
    properties=None, on_failure='finish', notify_early=False, emails=None):
    """Launch a workflow.

    :param name: Name of the project.
    :param flow: Name of the workflow.
    :param jobs: List of names of jobs to run (run entire workflow by default).
    :param concurrent: Run workflow concurrently with any previous executions.
    :param properties: Dictionary that will override global properties in this
      execution of the workflow. This dictionary will be flattened similarly to
      how :class:`~azkaban.job.Job` options are handled.
    :param on_failure: Set the execution behavior on job failure. Available
      options: `'finish'` (finish currently running jobs, but do not start any
      others), `'continue'` (continue executing jobs as long as dependencies
      are met),`'cancel'` (cancel all jobs immediately).
    :param notify_early: Send any notification emails when the first job fails
      rather than when the entire workflow finishes.
    :param emails: List of emails or pair of list of emails to be notified
      when the flow fails. Note that this will override any properties set in
      the worfklow. If a single list is passed, the emails will be used for
      both success and failure events. If a pair of lists is passed, the first
      will receive failure emails, the second success emails.

    Note that in order to run a workflow on Azkaban, it must already have been
    uploaded and the corresponding user must have permissions to run it.

    """
    if not jobs:
      disabled = '[]'
    else:
      all_names = set(
        n['id']
        for n in self.get_workflow_info(name, flow)['nodes']
      )
      run_names = set(jobs)
      missing_names = run_names - all_names
      if missing_names:
        raise AzkabanError(
          'Jobs not found in flow %r: %s.' %
          (flow, ', '.join(missing_names))
        )
      else:
        disabled = (
          '[%s]'
          % (','.join('"%s"' % (n, ) for n in all_names - run_names), )
        )
    try:
      failure_action = {
        'finish': 'finishCurrent',
        'continue': 'finishPossible',
        'cancel': 'cancelImmediately',
      }[on_failure]
    except KeyError:
      raise AzkabanError('Invalid `on_failure` value: %r.', on_failure)
    request_data = {
      'ajax': 'executeFlow',
      'project': name,
      'flow': flow,
      'disabled': disabled,
      'concurrentOption': 'concurrent' if concurrent else 'skip',
      'failureAction': failure_action,
      'notifyFailureFirst': 'true' if notify_early else 'false',
    }
    if properties:
      request_data.update(dict(
        ('flowOverride[%s]' % (key, ), value)
        for key, value in flatten(properties).items()
      ))
    if emails:
      if isinstance(emails[0], basestring):
        failure_emails = ','.join(emails)
        success_emails = failure_emails
      else:
        failure_emails = ','.join(emails[0])
        success_emails = ','.join(emails[1])
      request_data.update({
        'failureEmails': failure_emails,
        'failureEmailsOverride': 'true',
        'successEmails': success_emails,
        'successEmailsOverride': 'true',
      })
    return _extract_json(self._request(
      method='POST',
      endpoint='executor',
      use_cookies=False,
      data=request_data,
    ))

  def upload_project(self, name, path):
    """Upload project archive.

    :param name: Project name.
    :param path: Path to zip archive.

    """
    if not exists(path):
      raise AzkabanError('Unable to find archive at %r.' % (path, ))
    return _extract_json(self._request(
      method='POST',
      endpoint='manager',
      use_cookies=False,
      check_first=True,
      data={
        'ajax': 'upload',
        'project': name,
      },
      files={
        'file': ('file.zip', open(path, 'rb').read(), 'application/zip'),
      },
    ))

  def get_workflow_info(self, name, flow):
    """Get list of jobs corresponding to a workflow.

    :param name: Project name.
    :param flow: Name of flow in project.

    """
    raw_res = self._request(
      method='GET',
      endpoint='manager',
      params={
        'ajax': 'fetchflowjobs',
        'project': name,
        'flow': flow,
      },
    )
    try:
      return _extract_json(raw_res)
    except ValueError:
      raise AzkabanError('Flow %r not found.' % (flow, ))


class Execution(object):

  """Remote workflow execution.

  :param session: :class:`Session` instance.
  :param exec_id: Execution ID.

  """

  def __init__(self, session, exec_id):
    self._session = session
    self.exec_id = exec_id

  @property
  def status(self):
    """Execution status."""
    return self._session.get_execution_status(self.exec_id)

  @property
  def url(self):
    """Execution URL."""
    return '%s/executor?execid=%s' % (self._session.url, self.exec_id)

  def cancel(self):
    """Cancel execution."""
    self._session.cancel_execution(self.exec_id)

  def logs(self, delay=5):
    """Execution log generator.

    :param delay: time in seconds between each server poll

    Yields line by line.

    """
    finishing = False
    offset = 0
    while True:
      sleep(delay)
      logs = self._session.get_execution_logs(
        exec_id=self.exec_id,
        offset=offset,
      )
      if logs['length']:
        offset += logs['length']
        lines = (e for e in logs['data'].split('\n') if e)
        for line in lines:
          yield line
      elif finishing:
        break
      else:
        if self.status['status'] != 'RUNNING':
          finishing = True

  def job_logs(self, job, delay=5):
    """Job log generator.

    :param job: job name
    :param delay: time in seconds between each server poll

    Yields line by line.

    """
    finishing = False
    offset = 0
    while True:
      sleep(delay)
      logs = self._session.get_job_logs(
        exec_id=self.exec_id,
        job=job,
        offset=offset,
      )
      if logs['length']:
        offset += logs['length']
        lines = (e for e in logs['data'].split('\n') if e)
        for line in lines:
          yield line
      elif finishing:
        break
      else:
        running_jobs = set(
          e['id']
          for e in self.status['nodes']
          if e['status'] == 'RUNNING'
        )
        if job not in running_jobs:
          finishing = True

  @classmethod
  def start(cls, session, *args, **kwargs):
    """Convenience method to start a new execution.

    :param session: :class:`Session` instance.
    :param args: Cf. :meth:`Session.run_workflow`.
    :param kwargs: Cf. :meth:`Session.run_workflow`.

    """
    res = session.run_workflow(*args, **kwargs)
    return cls(session, res['execid'])
