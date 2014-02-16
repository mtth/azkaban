#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban session module."""


from azkaban.ext.pig import PigJob
from azkaban.project import Project
from azkaban.job import Job
from azkaban.session import Session
from azkaban.util import AzkabanError, Config, temppath
from ConfigParser import NoOptionError, NoSectionError
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

  def setup(self):
    if not self.session:
      raise SkipTest
    if self.project_name:
      self.project = Project(self.project_name)
      self.session.create_project(self.project, 'Testing project.')

  def teardown(self):
    sleep(3)
    if self.project:
      try:
        self.session.delete_project(self.project)
      except AzkabanError:
        pass # project was already deleted


class TestCreateDelete(_TestSession):

  project_name = 'azkabancli_test_create_delete'

  def setup(self):
    if not self.session:
      raise SkipTest
    if self.project_name:
      self.project = Project(self.project_name)
      # don't create project automatically (goal of these tests...)

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

  def test_create_project(self):
    ok_(not self.project_exists(self.project))
    self.session.create_project(self.project, 'Some description.')
    ok_(self.project_exists(self.project))

  @raises(AzkabanError)
  def test_create_duplicate_project(self):
    self.session.create_project(self.project, 'Some description.')
    self.session.create_project(self.project, 'Some other description.')

  def test_delete_project(self):
    self.session.create_project(self.project, 'Some description.')
    self.session.delete_project(self.project)
    ok_(not self.project_exists(self.project))

  @raises(AzkabanError)
  def test_delete_nonexistent_project(self):
    ok_(not self.project_exists(self.project))
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
      eq_(['projectId', 'version'], res.keys())

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
      self.project.add_job('foo', PigJob(path))
      with temppath() as path:
        self.project.build(path)
        res = self.session.upload_project(self.project, path)
    eq_(['projectId', 'version'], sorted(res.keys()))


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
    self.session.run_workflow(self.project, 'foo')

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
    self.session.run_workflow(self.project, 'foo', skip=True)

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
