#!/usr/bin/env python
# encoding: utf-8

"""Azkaban remote interaction module.

This contains the `Session` class which will be used for all interactions with
a remote Azkaban server.

"""

from .util import AzkabanError, Config, Adapter, MultipartForm, flatten
from getpass import getpass, getuser
from os.path import basename, exists
from requests.exceptions import HTTPError
from six import string_types
from six.moves.configparser import NoOptionError, NoSectionError
from six.moves.urllib.parse import urlparse
from time import sleep
import logging as lg
import requests as rq
import re


_logger = lg.getLogger(__name__)


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
      raise AzkabanError('Unable to connect to Azkaban server %r.', url)
    except rq.exceptions.MissingSchema:
      raise AzkabanError('Invalid Azkaban server url: %r.', url)
    else:
      return response

def _extract_json(response):
  """Extract JSON from Azkaban response, gracefully handling errors.

  :param response: Request response object.

  """
  try:
    json = response.json()
  except ValueError as err: # this should never happen
    _logger.error('No JSON decoded from response:\n%s', response.text)
    raise err
  else:
    if 'error' in json:
      raise AzkabanError(json['error'])
    elif json.get('status') == 'error':
      raise AzkabanError(json['message'])
    else:
      return json

def _parse_url(url):
  """Parse url, returning tuple of (username, password, address)

  :param url: HTTP endpoint (including protocol, port, and optional user /
    password).

  Supported url formats:

  + protocol://host:port
  + protocol://user@host:port
  + protocol://user:password@host:port
  + user@protocol://host:port (compatibility with older versions)
  + user:password@protocol://host:port (compatibility with older versions)

  """
  if not re.match(r'[a-zA-Z]+://', url) and not re.search(r'@[a-zA-Z]+://', url):
    # no scheme specified, default to http://
    url = 'http://' + url
  if re.search(r'@[a-zA-Z]+://', url):
    # compatibility mode: `user@protocol://host:port` or
    # `user:password@protocol://host:port`
    splitted = url.rstrip('/').split('@')
    if len(splitted) == 1:
      address = splitted[0]
      user = None
      password = None
    elif len(splitted) == 2:
      address = splitted[1]
      creds = splitted[0].split(':', 1)
      if len(creds) == 1:
        user = creds[0]
        password = None
      else:
        user, password = creds
    else:
      raise AzkabanError('Malformed url: %r' % (url, ))
    return user, password, address
  else:
    parsed = urlparse(url)
    return (parsed.username, parsed.password,
            '%s://%s:%s' % (parsed.scheme, parsed.hostname, parsed.port))

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
  :param attempts: Maximum number of attempts to refresh session.

  This class contains mostly low-level methods that translate directly into
  Azkaban API calls. The :class:`~azkaban.remote.Execution` class should be
  preferred for interacting with workflow executions.

  Note that each session's ID is lazily updated. In particular, instantiating
  the :class:`Session` doesn't guarantee that its current ID (e.g. loaded from
  the configuration file) is valid.

  """

  def __init__(self, url=None, alias=None, attempts=3):
    self.attempts = attempts
    self.config = Config()
    if not url:
      alias = alias or self.config.get_option('azkaban', 'alias')
      url = _resolve_alias(self.config, alias)
    self.user, self.password, self.url = _parse_url(url)
    if not self.user:
      self.user = getuser()
    self.id = _get_session_id(self.config, str(self).replace(':', '.'))
    self._logger = Adapter(repr(self), _logger)
    self._logger.debug('Instantiated.')

  def __repr__(self):
    return '<%s(url=\'%s@%s\')>' % (
      self.__class__.__name__, self.user, self.url
    )

  def __str__(self):
    return '%s@%s' % (self.user, self.url)

  def is_valid(self, response=None):
    """Check if the current session ID is valid.

    :param response: If passed, this reponse will be used to determine the
      validity of the session. Otherwise a simple test request will be emitted.

    """
    self._logger.debug('Checking if current session is valid.')
    if not self.id:
      self._logger.debug('No previous ID found.')
      return False
    if response is None:
      # issue a request to check if the ID is valid (note the explicit `None`
      # check as 500 responses are falsish).
      self._logger.debug('Checking if ID %s is valid.', self.id)
      response = _azkaban_request(
        'POST',
        '%s/manager' % (self.url, ),
        data={'session.id': self.id},
      )
      # the above request will return a 200 empty response if the current
      # session ID is valid and a 500 response otherwise
    if (
      '<!-- /.login -->' in response.text or # usual non API error response
      'Login error' in response.text or # special case for API
      '"error" : "session"' in response.text # error when running a flow's jobs
    ):
      self._logger.debug('ID %s is invalid:\n%s', self.id, response.text)
      return False
    else:
      self._logger.debug('ID %s is valid.', self.id)
      return True

  def get_workflow_executions(self, project, flow, start=0, length=10):
    """Fetch executions of a flow.

    :param project: Project name.
    :param flow: Flow name.
    :param start: Start index (inclusive) of the returned list.
    :param length: Max length of the returned list.

    """
    self._logger.debug('Fetching executions of %s/%s.', project, flow)
    res = self._request(
      method='GET',
      endpoint='manager',
      params={
        'ajax': 'fetchFlowExecutions',
        'project': project,
        'flow': flow,
        'start': start,
        'length': length
      },
    )
    if not res.text:
      # Azkaban returns a 200 empty response if the project doesn't exist so
      # we throw an explicit error here, rather than letting `_extract_json`
      # fail generically.
      raise AzkabanError(
        'Unable to fetch executions. Check that project %r exists.', project
      )
    else:
      return _extract_json(res)

  def get_running_workflows(self, project, flow):
    """Get running executions of a flow.

    :param project: Project name.
    :param flow: Flow name.

    Note that if the project doesn't exist, the Azkaban server will return a
    somewhat cryptic error `Project 'null' not found.`, even though the name of
    the project isn't `null`.

    """
    self._logger.debug('Fetching running executions of %s/%s.', project, flow)
    return _extract_json(self._request(
      method='GET',
      endpoint='executor',
      params={
        'ajax': 'getRunning',
        'project': project,
        'flow': flow,
      },
    ))

  def get_execution_status(self, exec_id):
    """Get status of an execution.

    :param exec_id: Execution ID.

    """
    self._logger.debug('Fetching status for execution %s.', exec_id)
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
    self._logger.debug('Fetching logs for execution %s.', exec_id)
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
    self._logger.debug('Fetching logs for execution %s, job %s.', exec_id, job)
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
    self._logger.debug('Cancelling execution %s.', exec_id)
    res = _extract_json(self._request(
      method='GET',
      endpoint='executor',
      params={
        'execid': exec_id,
        'ajax': 'cancelFlow',
      },
    ))
    if 'error' in res:
      raise AzkabanError('Execution %s is not running.', exec_id)
    else:
      self._logger.info('Execution %s cancelled.', exec_id)
    return res

  def create_project(self, name, description):
    """Create project.

    :param name: Project name.
    :param description: Project description.

    """
    self._logger.debug('Creating project %s.', name)
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
    self._logger.debug('Deleting project %s.', name)
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
    self._logger.debug('Starting project %s workflow %s.', name, flow)
    request_data = {
      'ajax': 'executeFlow',
      'project': name,
      'flow': flow
    }
    request_data.update(self._run_options(
      name,
      flow,
      jobs=jobs,
      concurrent=concurrent,
      properties=properties,
      on_failure=on_failure,
      notify_early=notify_early,
      emails=emails
    ))
    res = _extract_json(self._request(
      method='POST',
      endpoint='executor',
      include_session='params',
      data=request_data,
    ))
    self._logger.info('Started project %s workflow %s.', name, flow)
    return res

  def schedule_workflow(self, name, flow, date, time, period=None, **kwargs):
    """Schedule a workflow.

    :param name: Project name.
    :param flow: Name of flow in project.
    :param date: Date of the first run (possible values:
      `'08/07/2014'`, `'12/11/2015'`).
    :param time: Time of the schedule (possible values:
      `'9,21,PM,PDT'`, `'10,30,AM,PDT'`).
    :param period: Frequency to repeat. Consists of a number and a unit
      (possible values: `'1s'`, `'2m'`, `'3h'`, `'2M'`). If not specified
      the flow will be run only once.
    :param \*\*kwargs: See :meth:`run_workflow` for documentation.

    """
    self._logger.debug('Scheduling project %s workflow %s.', flow, name)
    request_data = {
      'ajax': 'scheduleFlow',
      'projectName': name,
      'projectId': self._get_project_id(name),
      'flow': flow,
      'scheduleDate': date,
      'scheduleTime': time,
      'is_recurring': 'on' if period else 'off',
    }
    if period:
      request_data['period'] = period
    request_data.update(self._run_options(name, flow, **kwargs))
    res = _extract_json(self._request(
      method='POST',
      endpoint='schedule',
      data=request_data,
    ))
    self._logger.info('Scheduled project %s workflow %s.', name, flow)
    return res

  def unschedule_workflow(self, name, flow):
    """Unschedule a workflow.

    :param name: Project name.
    :param flow: Name of flow in project.

    """
    self._logger.debug('Unscheduling project %s workflow %s.', flow, name)
    request_data = {
      'action': 'removeSched',
      'scheduleId': self.get_schedule(name, flow)['scheduleId'],
    }
    res = _extract_json(self._request(
      method='POST',
      endpoint='schedule',
      data=request_data,
    ))
    self._logger.info('Unscheduled project %s workflow %s.', name, flow)
    return res

  def get_schedule(self, name, flow):
    """Get schedule information.

    :param name: Project name.
    :param flow: Name of flow in project.

    """
    self._logger.debug(
      'Retrieving schedule for project %s workflow %s.', flow, name
    )
    res = _extract_json(self._request(
      method='GET',
      endpoint='schedule',
      params={
        'ajax': 'fetchSchedule',
        'projectId': self._get_project_id(name),
        'flowId': flow,
      },
    ))
    self._logger.info(
      'Retrieved schedule for project %s workflow %s.', name, flow
    )
    if 'schedule' not in res:
      raise AzkabanError(
        'Failed to get schedule. Check that the schedule exists.'
      )
    return res['schedule']

  def _get_project_id(self, name):
    """Fetch the id of a project.

    :param name: Project name.

    """
    self._logger.debug('Retrieving id for project %s.', name)
    try:
      res = _extract_json(self._request(
        method='GET',
        endpoint='manager',
        params={
          # there is no endpoint to get the project id, getPermissions is
          # the least expensive endpoint whose response contains the id
          'ajax': 'getPermissions',
          'project': name,
        },
      ))
    except ValueError:
      # Azkaban server sends a 200 empty response if the project doesn't exist
      raise AzkabanError(
        'Failed to get project id. Check that the project exists.'
      )
    else:
      project_id = res['projectId']
    self._logger.info('Retrieved id for project %s: %s.', name, project_id)
    return project_id

  def upload_project(self, name, path, archive_name=None, callback=None):
    """Upload project archive.

    :param name: Project name.
    :param path: Local path to zip archive.
    :param archive_name: Filename used for the archive uploaded to Azkaban.
      Defaults to `basename(path)`.
    :param callback: Callback forwarded to the streaming upload.

    """
    self._logger.debug('Uploading archive %r to project %s.', path, name)
    if not exists(path):
      raise AzkabanError('Unable to find archive at %r.' % (path, ))
    if not self.is_valid():
      self._refresh() # ensure that the ID is valid
    archive_name = archive_name or basename(path)
    form = MultipartForm(
      files=[{
        'path': path,
        'name': archive_name,
        'type': 'application/zip' # force this (tempfiles don't have extension)
      }],
      params={
        'ajax': 'upload',
        'project': name,
        'session.id': self.id,
      },
      callback=callback
    )
    # note that we have made sure the ID is valid, for two reasons:
    # + to avoid reuploading large files
    # + to simplify the custom ID update process (form parameter)
    res = _extract_json(self._request(
      method='POST',
      endpoint='manager',
      include_session=False,
      headers=form.headers,
      data=form,
    ))
    self._logger.info(
      'Archive %s for project %s uploaded as %s.', path, name, archive_name
    )
    return res

  def get_workflow_info(self, name, flow):
    """Get list of jobs corresponding to a workflow.

    :param name: Project name.
    :param flow: Name of flow in project.

    """
    self._logger.debug(
      'Fetching infos for workflow %s in project %s', flow, name
    )
    try:
      res = self._request(
        method='GET',
        endpoint='manager',
        params={
          'ajax': 'fetchflowjobs',
          'project': name,
          'flow': flow,
        },
      )
    except HTTPError:
      # the Azkaban server throws a NullPointerException if the flow doesn't
      # exist in the project, which causes a 500 response
      raise AzkabanError('Worklow %s not found in project %s.', flow, name)
    else:
      try:
        return _extract_json(res)
      except ValueError:
        # but sends a 200 empty response if the project doesn't exist
        raise AzkabanError('Project %s not found.', name)

  def _refresh(self, password=None):
    """Refresh session ID.

    :param password: Password used to log into Azkaban. If not specified,
      will prompt for one.

    Also caches the session ID for future use.

    """
    self._logger.debug('Refreshing.')
    attempts = self.attempts
    password = password or self.password
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
        self._logger.warning('Invalid login attempt.')
        attempts -= 1
        password = None
        if attempts <= 0:
          raise AzkabanError('Too many unsuccessful login attempts. Aborting.')
      else:
        break
    self.id = res['session.id']
    self.config.parser.set('session_id', str(self).replace(':', '.'), self.id)
    self.config.save()
    self._logger.info('Refreshed.')

  def _run_options(self, name, flow, jobs=None, concurrent=True,
    properties=None, on_failure='finish', notify_early=False, emails=None):
    """Construct data dict for run related actions.

    See :meth:`run_workflow` for parameter documentation.

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
      raise ValueError('Invalid `on_failure` value: %r.' % (on_failure, ))
    request_data = {
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
      if isinstance(emails[0], string_types):
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
    return request_data

  def _request(self, method, endpoint, include_session='cookies', **kwargs):
    """Make a request to Azkaban using this session.

    :param method: HTTP method.
    :param endpoint: Server endpoint (e.g. manager).
    :param include_session: Where to include the `session_id` (possible values:
      `'cookies'`, `'params'`, `False`).
    :param kwargs: Keyword arguments passed to :func:`_azkaban_request`.

    If the session expired, will prompt for a password to refresh.

    """
    full_url = '%s/%s' % (self.url, endpoint.lstrip('/'))

    if not self.id:
      self._logger.debug('No ID found.')
      self._refresh()

    def _send_request():
      """Try sending the request with the appropriate credentials."""
      if include_session == 'cookies':
        kwargs.setdefault('cookies', {})['azkaban.browser.session.id'] = self.id
      elif include_session == 'params':
        kwargs.setdefault('data', {})['session.id'] = self.id
      elif include_session:
        raise ValueError('Invalid `include_session`: %r' % (include_session, ))
      return _azkaban_request(method, full_url, **kwargs)

    response = _send_request()
    if not self.is_valid(response):
      self._refresh()
      response = _send_request()

    # `_refresh` raises an exception rather than letting an unauthorized second
    # request happen. this means that something is wrong with the server.
    if not self.is_valid(response):
      raise AzkabanError('Azkaban server is unavailable.')

    try:
      response.raise_for_status() # check that we get a 2XX response back
    except HTTPError as err: # catch, log, and reraise
      self._logger.warning(
        'Received invalid response from %s:\n%s',
        response.request.url, response.content
      )
      raise err
    else:
      return response


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
      sleep(delay)

  def job_logs(self, job, delay=5):
    """Job log generator.

    :param job: job name
    :param delay: time in seconds between each server poll

    Yields line by line.

    """
    finishing = False
    offset = 0
    while True:
      try:
        logs = self._session.get_job_logs(
          exec_id=self.exec_id,
          job=job,
          offset=offset,
        )
      except HTTPError as err:
        # if Azkaban is hanging, the job might be stuck in preparing stage
        preparing = False
        while True:
          sleep(delay)
          preparing_jobs = set(
            e['id']
            for e in self.status['nodes']
            if e['status'] == 'PREPARING'
          )
          if job in preparing_jobs:
            if not preparing:
              preparing = True
              _logger.debug(
                'Job %s in execution %s is still preparing.', job, self.exec_id
              )
          else:
            break
        if not preparing:
          # something else is causing the error
          raise err
      else:
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
      sleep(delay)

  @classmethod
  def start(cls, session, *args, **kwargs):
    """Convenience method to start a new execution.

    :param session: :class:`Session` instance.
    :param args: Cf. :meth:`Session.run_workflow`.
    :param kwargs: Cf. :meth:`Session.run_workflow`.

    """
    res = session.run_workflow(*args, **kwargs)
    return cls(session, res['execid'])
