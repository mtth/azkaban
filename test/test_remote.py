#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban remote module."""


from azkaban.ext.pig import PigJob
from azkaban.project import Project
from azkaban.job import Job
from azkaban.remote import Execution, Session
from azkaban.util import AzkabanError, Config, temppath
from six.moves.configparser import NoOptionError, NoSectionError
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest
from time import sleep


class _TestSession(object):

  """Base class to run tests on an Azkaban server.

  These will be skipped if no valid credentials (url and associated session id)
  are found.

  If the class variable `project` is specified, the corresponding project
  will be created before each test.

  """

  project_name = None
  session = None

  @classmethod
  def setup_class(cls):
    if not cls.session:
      config = Config()
      try:
        alias = config.parser.get('azkaban', 'test.alias')
      except (NoOptionError, NoSectionError):
        pass
      else:
        cls.session = Session(alias=alias)
        try:
          cls.session.create_project(cls.project_name, 'Testing project.')
        except AzkabanError:
          pass # project already exists somehow

  @classmethod
  def teardown_class(cls):
    if cls.session:
      try:
        cls.session.delete_project(cls.project_name)
      except AzkabanError:
        pass # project was already deleted

  def setup(self):
    if not self.session:
      raise SkipTest
    if self.project_name:
      sleep(2)
      self.project = Project(self.project_name)


class TestCreateDelete(_TestSession):

  project_name = 'azkabancli_test_create_delete'

  @classmethod
  def setup_class(cls):
    # don't create project automatically (goal of these tests...)
    if not cls.session:
      config = Config()
      try:
        alias = config.parser.get('azkaban', 'test.alias')
      except (NoOptionError, NoSectionError):
        pass
      else:
        cls.session = Session(alias=alias)

  def project_exists(self, project):
    try:
      try:
        project.add_job('test', Job({'type': 'noop'}))
      except AzkabanError:
        pass # job was already added
      with temppath() as path:
        project.build(path)
        self.session.upload_project(project, path)
    except AzkabanError:
      return False
    else:
      return True

  def test_create_delete_project(self):
    ok_(not self.project_exists(self.project))
    self.session.create_project(self.project, 'Some description.')
    ok_(self.project_exists(self.project))
    self.session.delete_project(self.project)
    ok_(not self.project_exists(self.project))

  @raises(AzkabanError)
  def test_create_duplicate_project(self):
    self.session.create_project(self.project, 'Some description.')
    self.session.create_project(self.project, 'Some other description.')

  @raises(AzkabanError)
  def test_delete_nonexistent_project(self):
    if self.project_exists(self.project):
      self.session.delete_project(self.project)
    self.session.delete_project(self.project)


class TestUpload(_TestSession):

  project_name = 'azkabancli_test_upload'

  @raises(AzkabanError)
  def test_missing_archive(self):
    self.session.upload_project(self.project, 'foo')

  @raises(AzkabanError)
  def test_invalid_project(self):
    project = Project('an_non_existent_project')
    with temppath() as path:
      project.add_job('test', Job({'type': 'noop'}))
      project.build(path)
      self.session.upload_project(project, path)

  def test_upload_simple(self):
    with temppath() as path:
      self.project.add_job('test', Job({'type': 'noop'}))
      self.project.build(path)
      res = self.session.upload_project(self.project, path)
      eq_(['projectId', 'version'], sorted(res))

  @raises(AzkabanError)
  def test_upload_missing_type(self):
    with temppath() as path:
      self.project.add_job('test', Job())
      self.project.build(path)
      self.session.upload_project(self.project, path)

  def test_upload_pig_job(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      self.project.add_job('foo', PigJob({'pig.script': path}))
      with temppath() as path:
        self.project.build(path)
        res = self.session.upload_project(self.project, path)
    eq_(['projectId', 'version'], sorted(res))


class TestGetWorkflowInfo(_TestSession):

  project_name = 'azkaban_cli_test_flow_jobs'

  def get_job_names(self, flow_info):
    return [n['id'] for n in flow_info['nodes']]

  @raises(AzkabanError)
  def test_missing_project(self):
    self.session.get_workflow_info('some_missing_project', 'baz')

  @raises(AzkabanError)
  def test_invalid_flow(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    self.session.get_workflow_info(self.project, 'baz')

  def test_get_single_job(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    info = self.session.get_workflow_info(self.project, 'foo')
    eq_(self.get_job_names(info), ['foo'])

  def test_get_multiple_jobs(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    info = self.session.get_workflow_info(self.project, 'bar')
    eq_(sorted(self.get_job_names(info)), ['bar', 'foo'])


class TestGetProjectInfo(_TestSession):

  project_name = 'azkaban_cli_test_project_info'

  def test_valid_name(self):
    project_id = self.session._get_project_id(self.project_name)
    project_id2 = self.session._get_project_id(self.project_name)
    ok_(isinstance(project_id, int))
    eq_(project_id, project_id2)

  @raises(AzkabanError)
  def test_missing_name(self):
    self.session._get_project_id('DoesNotExist')


class TestRun(_TestSession):

  project_name = 'azkaban_cli_run'

  def test_run_simple_workflow(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    res = self.session.run_workflow(self.project, 'foo')
    eq_(['execid', 'flow', 'message', 'project'], sorted(res.keys()))
    eq_(res['message'][:32], 'Execution submitted successfully')

  def test_run_workflow_with_dependencies(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    res = self.session.run_workflow(self.project, 'bar')
    eq_(['execid', 'flow', 'message', 'project'], sorted(res.keys()))
    eq_(res['message'][:32], 'Execution submitted successfully')

  @raises(AzkabanError)
  def test_run_missing_workflow(self):
    self.session.run_workflow(self.project, 'foo2')

  @raises(AzkabanError)
  def test_run_non_workflow_job(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    res = self.session.run_workflow(self.project, 'foo')

  @raises(AzkabanError)
  def test_run_blocking_workflow(self):
    options = {'type': 'command', 'command': 'sleep 2'}
    self.project.add_job('foo', Job(options))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    self.session.run_workflow(self.project, 'foo')
    self.session.run_workflow(self.project, 'foo', concurrent=False)

  def test_run_non_blocking_workflow(self):
    options = {'type': 'command', 'command': 'sleep 2'}
    self.project.add_job('foo', Job(options))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    self.session.run_workflow(self.project, 'foo')
    res = self.session.run_workflow(self.project, 'foo')
    eq_(['execid', 'flow', 'message', 'project'], sorted(res.keys()))
    eq_(res['message'][:32], 'Flow foo is already running with')

  def test_run_fail_early(self):
    options = {'type': 'command', 'command': 'sleep ${time}'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'time': 5}))
    self.project.add_job('f', Job({'type': 'noop', 'dependencies': 'foo,bar'}))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    res = self.session.run_workflow(self.project, 'f', on_failure='cancel')
    exec_id = res['execid']
    sleep(2)
    eq_(self.session.get_execution_status(exec_id)['status'], 'FAILED')

  def test_run_fail_finish(self):
    options = {'type': 'command', 'command': 'sleep ${time}'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'time': 5}))
    self.project.add_job('f', Job({'type': 'noop', 'dependencies': 'foo,bar'}))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    res = self.session.run_workflow(self.project, 'f', on_failure='finish')
    eid = res['execid']
    sleep(2)
    eq_(self.session.get_execution_status(eid)['status'], 'FAILED_FINISHING')

  @raises(ValueError)
  def test_run_fail_invalid_on_failure(self):
    options = {'type': 'command', 'command': 'sleep ${time}'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'time': 5}))
    self.project.add_job('f', Job({'type': 'noop', 'dependencies': 'foo,bar'}))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    self.session.run_workflow(self.project, 'f', on_failure='foobar')

  @raises(AzkabanError)
  def test_run_wrong_job_in_workflow(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    self.session.run_workflow(self.project, 'foo', jobs=['bar'])

  def test_run_single_job_in_workflow(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    self.session.run_workflow(self.project, 'bar', jobs=['foo'])


class TestExecution(_TestSession):

  project_name = 'azkabancli_test_execution'

  def setup(self):
    super(TestExecution, self).setup()
    options = {'type': 'command', 'command': 'sleep 4'}
    self.project.add_job('foo', Job(options))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)

  def test_execution_start(self):
    exe = Execution.start(self.session, self.project, 'foo')
    sleep(2)
    eq_(exe.status['status'], 'RUNNING')
    sleep(2)
    eq_(exe.status['status'],'SUCCEEDED')

  def test_execution_cancel(self):
    exe = Execution.start(self.session, self.project, 'foo')
    sleep(1)
    exe.cancel()
    sleep(1)
    eq_(exe.status['status'],'KILLED')

  def test_execution_logs(self):
    exe = Execution.start(self.session, self.project, 'foo')
    logs = '\n'.join(exe.logs(2))
    ok_('Submitting job \'foo\' to run.' in logs)


class TestSchedule(_TestSession):

  project_name = 'azkabancli_test_schedule'

  def setup(self):
    super(TestSchedule, self).setup()
    options = {'type': 'command', 'command': 'sleep 4'}
    self.project.add_job('foo', Job(options))
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)

  def test_schedule_unschedule(self):
    res = self.session.schedule_workflow(
      self.project_name,
      'foo',
      '08/07/2014',
      '9,21,PM,PDT',
      '1d',
    )
    ok_(res['status'] == 'success')
    res = self.session.unschedule_workflow(self.project_name, 'foo')
    ok_(res['status'] == 'success')

  @raises(AzkabanError)
  def test_invalid_unschedule(self):
    res = self.session.unschedule_workflow('DoesNotExist', 'foo')

  @raises(AzkabanError)
  def test_invalid_schedule(self):
    res = self.session.schedule_workflow(
      self.project_name,
      'foo',
      '08/07/2014',
      '9,21PM,PDT',
      '1d',
    )


class TestProperties(_TestSession):

  project_name = 'azkabancli_test_properties'

  message = 'This is definitely a unique message.'
  override = 'This is even more definitely a unique message.'

  def _run_workflow(self, flow, **kwargs):
    with temppath() as path:
      self.project.build(path)
      self.session.upload_project(self.project, path)
    exe = Execution.start(self.session, self.project, flow, properties=kwargs)
    for i in range(5):
      # wait until workflow is launched
      sleep(1)
      try:
        status = exe.status
      except AzkabanError:
        pass
      else:
        if status['status'] != 'PREPARING':
          break
    return exe

  def _add_command_job(self, name, command, **kwargs):
    self.project.add_job(
      name,
      Job({'type': 'command', 'command': command}, kwargs),
    )

  def _add_flow_job(self, name, flow, **kwargs):
    self.project.add_job(
      name,
      Job({'type': 'flow', 'flow.name': flow}, kwargs),
    )

  def test_global_properties(self):
    self.project.properties = {'msg': self.message}
    self._add_command_job('foo', 'echo ${msg}')
    exe = self._run_workflow('foo')
    ok_(self.message in '\n'.join(exe.job_logs('foo', 1)))

  def test_missing_global_properties(self):
    self._add_command_job('foo', 'echo ${msg}')
    exe = self._run_workflow('foo')
    eq_(exe.status['status'], 'FAILED')

  def test_options_override_global_properties(self):
    self.project.properties = {'msg': self.message}
    self._add_command_job('foo', 'echo ${msg}', msg=self.override)
    exe = self._run_workflow('foo')
    ok_(self.override in '\n'.join(exe.job_logs('foo', 1)))

  def test_runtime_properties_override_global_properties(self):
    # runtime properties can be used to override .properties options
    self.project.properties = {'msg': self.message}
    self._add_command_job('foo', 'echo ${msg}')
    exe = self._run_workflow('foo', msg=self.override)
    ok_(self.override in '\n'.join(exe.job_logs('foo', 1)))

  def test_options_override_runtime_properties(self):
    # but runtime properties don't override .job options
    self._add_command_job('foo', 'echo ${msg}', msg=self.message)
    exe = self._run_workflow('foo', msg=self.override)
    ok_(self.message in '\n'.join(exe.job_logs('foo', 1)))

  def test_embedded_properties(self):
    # note the colon separated notation for embedded flows
    self._add_command_job('foo', 'echo ${msg}')
    self._add_flow_job('bar', 'foo', msg=self.message)
    exe = self._run_workflow('bar')
    ok_(self.message in '\n'.join(exe.job_logs('bar:foo', 1)))

  def test_embedded_properties_propagate(self):
    # embedded flow properties also propagate to nested flows
    self._add_command_job('foo', 'echo ${msg}')
    self._add_flow_job('flow1', 'foo')
    self._add_flow_job('flow2', 'flow1', msg=self.message)
    exe = self._run_workflow('flow2')
    ok_(self.message in '\n'.join(exe.job_logs('flow2:flow1:foo', 1)))

  def test_embedded_properties_override_propagated_embedded_properties(self):
    # embedded flow properties also propagate to nested flows
    self._add_command_job('foo', 'echo ${msg}')
    self._add_flow_job('flow1', 'foo', msg=self.override)
    self._add_flow_job('flow2', 'flow1', msg=self.message)
    exe = self._run_workflow('flow2')
    ok_(self.override in '\n'.join(exe.job_logs('flow2:flow1:foo', 1)))

  def test_embedded_properties_override_global_properties(self):
    self.project.properties = {'msg': self.message}
    self._add_command_job('foo', 'echo ${msg}')
    self._add_flow_job('bar', 'foo', msg=self.override)
    exe = self._run_workflow('bar')
    ok_(self.override in '\n'.join(exe.job_logs('bar:foo', 1)))

  def test_embedded_properties_override_runtime_properties(self):
    # embedded flow properties override runtime properties (!)
    self._add_command_job('foo', 'echo ${msg}')
    self._add_flow_job('bar', 'foo', msg=self.override)
    exe = self._run_workflow('bar', msg=self.message)
    ok_(self.override in '\n'.join(exe.job_logs('bar:foo', 1)))

  def test_options_override_embedded_properties(self):
    # embedded flow properties don't override job options
    self._add_command_job('foo', 'echo ${msg}', msg=self.override)
    self._add_flow_job('bar', 'foo', msg=self.message)
    exe = self._run_workflow('bar')
    ok_(self.override in '\n'.join(exe.job_logs('bar:foo', 1)))
