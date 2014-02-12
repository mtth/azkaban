#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban project module."""

from azkaban.project import *
from azkaban.job import Job, PigJob
from azkaban.util import AzkabanError, flatten, temppath
from ConfigParser import RawConfigParser
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest
from os.path import relpath, abspath, join
from requests import ConnectionError, post
from time import sleep, time
from zipfile import ZipFile


class TestProject(object):

  def setup(self):
    self.project = Project('foo')

  def test_add_file(self):
    self.project.add_file(__file__, 'bar')
    eq_(self.project._files, {__file__: 'bar'})

  def test_add_relative_file(self):
    project = Project('foo', root=__file__)
    project.add_file('test_job.py', 'bar')
    eq_(project._files, {join(dirname(__file__), 'test_job.py'): 'bar'})

  @raises(AzkabanError)
  def test_missing_file(self):
    self.project.add_file('bar')

  @raises(AzkabanError)
  def test_relative_file(self):
    self.project.add_file(relpath(__file__))

  def test_add_duplicate_file(self):
    self.project.add_file(__file__)
    self.project.add_file(__file__)
    eq_(self.project._files, {__file__: None})

  @raises(AzkabanError)
  def test_add_inconsistent_duplicate_file(self):
    self.project.add_file(__file__)
    self.project.add_file(__file__, 'this.py')

  def test_add_job(self):
    class OtherJob(Job):
      test = None
      def on_add(self, project, name):
        self.test = (project.name, name)
    job = OtherJob()
    self.project.add_job('bar', job)
    eq_(job.test, ('foo', 'bar'))

  @raises(AzkabanError)
  def test_add_duplicate_job(self):
    self.project.add_job('bar', Job())
    self.project.add_job('bar', Job())

  def test_merge_project(self):
    job_bar = Job()
    self.project.add_job('bar', job_bar)
    file_bar = __file__
    self.project.add_file(file_bar, 'bar')
    project2 = Project('qux')
    job_baz = Job()
    project2.add_job('baz', job_baz) 
    file_baz = abspath('README.rst')
    project2.add_file(file_baz, 'baz')
    project2.merge_into(self.project)
    eq_(self.project.name, 'foo')
    eq_(self.project._jobs, {'bar': job_bar, 'baz': job_baz})
    eq_(self.project._files, {file_bar: 'bar', file_baz: 'baz'})

  @raises(AzkabanError)
  def test_build_empty(self):
    with temppath() as path:
      self.project.build(path)

  def test_build_single_job(self):
    class OtherJob(Job):
      test = None
      def on_build(self, project, name):
        self.test = (project.name, name)
    job = OtherJob({'a': 2})
    self.project.add_job('bar', job)
    with temppath() as path:
      self.project.build(path)
      eq_(job.test, ('foo', 'bar'))
      reader =  ZipFile(path)
      try:
        ok_('bar.job' in reader.namelist())
        eq_(reader.read('bar.job'), 'a=2\n')
      finally:
        reader.close()

  def test_build_with_file(self):
    self.project.add_file(__file__.rstrip('c'), 'this.py')
    with temppath() as path:
      self.project.build(path)
      reader = ZipFile(path)
      try:
        ok_('this.py' in reader.namelist())
        eq_(reader.read('this.py').split('\n')[0], '#!/usr/bin/env python')
      finally:
        reader.close()

  def test_build_multiple_jobs(self):
    self.project.add_job('foo', Job({'a': 2}))
    self.project.add_job('bar', Job({'b': 3}))
    self.project.add_file(__file__, 'this.py')
    with temppath() as path:
      self.project.build(path)
      reader = ZipFile(path)
      try:
        ok_('foo.job' in reader.namelist())
        ok_('bar.job' in reader.namelist())
        ok_('this.py' in reader.namelist())
        eq_(reader.read('foo.job'), 'a=2\n')
      finally:
        reader.close()

  @raises(AzkabanError)
  def test_missing_alias(self):
    self.project.get_session('foo', alias='bar')


class _TestServer(object):

  """Base class to run tests on an Azkaban server.

  These will be skipped if no valid credentials (url and associated session id)
  are found.

  If the class variable `project_name` is specified, the corresponding project
  will be created before each test.

  """

  project_name = None
  session_id = None
  url = None

  @classmethod
  def setup_class(cls):
    parser = RawConfigParser()
    parser.read(Project.rcpath)
    for section in parser.sections():
      url = parser.get(section, 'url').rstrip('/')
      if parser.has_option(section, 'session_id'):
        session_id = parser.get(section, 'session_id')
        try:
          if not post(
            '%s/manager' % (url, ),
            {'session.id': session_id},
            verify=False
          ).text:
            cls.session_id = session_id
            cls.url = url
            return
        except ConnectionError:
          # skip tests if no valid credentials found
          pass

  def setup(self):
    if not self.session_id:
      raise SkipTest
    if self.project_name:
      self.project = Project(self.project_name)
      self.project.create('testing project', self.url, self.session_id)

  def teardown(self):
    sleep(3)
    if self.project_name:
      self.project.delete(self.url, self.session_id)


class TestCreateDelete(_TestServer):

  def setup(self):
    super(TestCreateDelete, self).setup()
    self.projects = []

  def teardown(self):
    super(TestCreateDelete, self).teardown()
    for project in self.projects:
      project.delete(self.url, self.session_id)

  def project_exists(self, project):
    try:
      try:
        project.add_job('test', Job({'type': 'noop'}))
      except AzkabanError:
        pass # job was already added
      with temppath() as archive:
        project.build(archive)
        project.upload(archive, self.url, self.session_id)
    except AzkabanError:
      return False
    else:
      return True

  def test_create_project(self):
    project = Project('azkabancli_foo')
    self.projects = [project]
    ok_(not self.project_exists(project))
    project.create('desc', self.url, self.session_id)
    ok_(self.project_exists(project))

  @raises(AzkabanError)
  def test_create_duplicate_project(self):
    project = Project('azkabancli_foo')
    self.projects = [project]
    project.create('desc', self.url, self.session_id)
    project.create('desc2', self.url, self.session_id)

  def test_delete_project(self):
    project = Project('azkabancli_foo')
    project.create('desc', self.url, self.session_id)
    project.delete(self.url, self.session_id)
    ok_(not self.project_exists(project))

  @raises(AzkabanError)
  def test_delete_nonexistent_project(self):
    project = Project('azkabancli_foo')
    ok_(not self.project_exists(project))
    project.delete(self.url, self.session_id)


class TestUpload(_TestServer):

  project_name = 'azkaban_cli_upload'

  @raises(AzkabanError)
  def test_missing_archive(self):
    self.project.upload('foo', self.url, self.session_id)

  @raises(AzkabanError)
  def test_invalid_project(self):
    project = Project('foobarzz')
    with temppath() as archive:
      project.add_job('test', Job({'type': 'noop'}))
      project.build(archive)
      project.upload(archive, self.url, self.session_id)

  @raises(AzkabanError)
  def test_bad_url(self):
    self.project.get_session('http://foo', password='bar')

  @raises(AzkabanError)
  def test_missing_protocol(self):
    self.project.get_session('foo', password='bar')

  @raises(AzkabanError)
  def test_bad_password(self):
    self.project.get_session(self.url, password='bar')

  def test_upload_simple(self):
    with temppath() as archive:
      self.project.add_job('test', Job({'type': 'noop'}))
      self.project.build(archive)
      res = self.project.upload(archive, self.url, self.session_id)
      eq_(['projectId', 'version'], res.keys())

  @raises(AzkabanError)
  def test_upload_missing_type(self):
    with temppath() as archive:
      self.project.add_job('test', Job())
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)

  def test_upload_pig_job(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      self.project.add_job('foo', PigJob(path))
      with temppath() as archive:
        self.project.build(archive)
        res = self.project.upload(archive, self.url, self.session_id)
        eq_(['projectId', 'version'], sorted(res.keys()))


class TestGetFlowJobs(_TestServer):

  project_name = 'azkaban_cli_flow_jobs'

  @raises(AzkabanError)
  def test_get_invalid_flow(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    self.project.get_flow_jobs('baz', self.url, self.session_id)

  def test_get_single_job(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    jobs = self.project.get_flow_jobs('foo', self.url, self.session_id)
    eq_(jobs, ['foo'])

  def test_get_multiple_jobs(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    jobs = self.project.get_flow_jobs('bar', self.url, self.session_id)
    eq_(sorted(jobs), ['bar', 'foo'])


class TestRun(_TestServer):

  project_name = 'azkaban_cli_run'

  def test_run_simple_workflow(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    res = self.project.run('foo', self.url, self.session_id)
    eq_(['execid', 'flow', 'message', 'project'], sorted(res.keys()))
    eq_(res['message'][:32], 'Execution submitted successfully')

  def test_run_workflow_with_dependencies(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    res = self.project.run('bar', self.url, self.session_id)
    eq_(['execid', 'flow', 'message', 'project'], sorted(res.keys()))
    eq_(res['message'][:32], 'Execution submitted successfully')

  @raises(AzkabanError)
  def test_run_missing_workflow(self):
    self.project.run('baz', self.url, self.session_id)

  @raises(AzkabanError)
  def test_run_non_workflow_job(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    self.project.run('foo', self.url, self.session_id)

  @raises(AzkabanError)
  def test_run_blocking_workflow(self):
    options = {'type': 'command', 'command': 'sleep 2'}
    self.project.add_job('foo', Job(options))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    self.project.run('foo', self.url, self.session_id)
    self.project.run('foo', self.url, self.session_id, block=True)

  def test_run_non_blocking_workflow(self):
    options = {'type': 'command', 'command': 'sleep 2'}
    self.project.add_job('foo', Job(options))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    self.project.run('foo', self.url, self.session_id)
    res = self.project.run('foo', self.url, self.session_id)
    eq_(['execid', 'flow', 'message', 'project'], sorted(res.keys()))
    eq_(res['message'][:32], 'Flow foo is already running with')

  @raises(AzkabanError)
  def test_run_wrong_job_in_workflow(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    self.project.run('foo', self.url, self.session_id, jobs=['bar'])

  def test_run_single_job_in_workflow(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, self.url, self.session_id)
    self.project.run('bar', self.url, self.session_id, jobs=['foo'])
