#!/usr/bin/env python
# encoding: utf-8

"""Azkaban test module."""

from azkaban import *
from ConfigParser import RawConfigParser
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest
from os.path import relpath, abspath
from time import sleep, time


class TestFlatten(object):

  def test_empty(self):
    eq_(flatten({}), {})

  def test_simple(self):
    dct = {'a': 1, 'B': 2}
    eq_(flatten(dct), dct)

  def test_nested(self):
    dct = {'a': 1, 'b': {'c': 3}}
    eq_(flatten(dct), {'a': 1, 'b.c': 3})


class TestProject(object):

  def setup(self):
    self.project = Project('foo')

  def test_add_file(self):
    self.project.add_file(__file__, 'bar')
    eq_(self.project._files, {__file__: 'bar'})

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
    file_baz = abspath('azkaban.py')
    project2.add_file(file_baz, 'baz')

    self.project.merge(project2)
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
        eq_(reader.read('this.py').split('\n')[5], 'from azkaban import *')
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
    self.project.upload('foo', alias='bar')


class TestJob(object):

  def test_options(self):
    eq_(Job().options, ())
    eq_(Job({'foo': 1}).options, ({'foo': 1}, ))
    eq_(Job({'foo': 1}, {}).options, ({'foo': 1}, {}))

  def test_build_options(self):
    eq_(Job().build_options, {})
    eq_(Job({'foo': 1}, {}).build_options, {'foo': '1'})
    eq_(Job({'foo': 1}, {'foo': 2}).build_options, {'foo': '2'})

  def test_generate_simple(self):
    job = Job({'a': 1, 'b': {'c': 2, 'd': 3}})
    with temppath() as path:
      job.build(path)
      with open(path) as reader:
        eq_(reader.read(), 'a=1\nb.c=2\nb.d=3\n')

  def test_generate_with_defaults(self):
    defaults = {'b': {'d': 4}, 'e': 5}
    job = Job(defaults, {'a': 1, 'b': {'c': 2, 'd': 3}})
    with temppath() as path:
      job.build(path)
      with open(path) as reader:
        eq_(reader.read(), 'a=1\nb.c=2\nb.d=3\ne=5\n')

  def test_generate_with_dependencies(self):
    foo = Job()
    bar = Job({'a': 3})
    job = Job({'a': 2, 'dependencies': 'bar,foo'})
    with temppath() as path:
      job.build(path)
      with open(path) as reader:
        eq_(reader.read(), 'a=2\ndependencies=bar,foo\n')


class TestPigJob(object):

  def test_init(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      job = PigJob(path, {'a': 2}, {'a': 3, 'b': 4}, {'type': 'noop'})
      with temppath() as tpath:
        job.build(tpath)
        with open(tpath) as reader:
          eq_(
            reader.read(),
            'a=3\nb=4\npig.script=%s\ntype=noop\n' % (path.lstrip('/'), )
          )

  def test_type(self):
    class OtherPigJob(PigJob):
      type = 'foo'
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      job = OtherPigJob(path, {'type': 'bar'})
      with temppath() as tpath:
        job.build(tpath)
        with open(tpath) as reader:
          eq_(
            reader.read(),
            'pig.script=%s\ntype=bar\n' % (path.lstrip('/'), )
          )

  @raises(AzkabanError)
  def test_missing(self):
    PigJob('foo.pig')

  def test_on_add(self):
    project = Project('pj')
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      project.add_job('foo', PigJob(path))
      eq_(project._files, {path: None})


class TestUpload(object):

  # requires valid credentials and an 'azkabancli' project on the server

  last_request = time()
  url = None
  valid_alias = None

  @classmethod
  def setup_class(cls):
    # skip tests if no valid credentials found
    parser = RawConfigParser()
    parser.read(Project.rcpath)
    for section in parser.sections():
      url = parser.get(section, 'url').rstrip('/')
      if parser.has_option(section, 'session_id'):
        session_id = parser.get(section, 'session_id')
        if not post(
          '%s/manager' % (url, ),
          {'session.id': session_id},
          verify=False
        ).text:
          cls.url = url
          cls.valid_alias = section
          return

  def wait(self, ms=2000):
    # wait before making a request
    delay = time() - self.last_request
    sleep(max(0, ms * 1e-3 - delay))
    self.last_request = time()

  def setup(self):
    if not self.valid_alias:
      raise SkipTest
    self.wait()
    self.project = Project('azkabancli')

  @raises(ValueError)
  def test_bad_parameters(self):
    self.project.upload('foo', user='bar')

  @raises(AzkabanError)
  def test_invalid_project(self):
    project = Project('foobarzz')
    project.upload('foo', alias=self.valid_alias)

  @raises(AzkabanError)
  def test_bad_url(self):
    self.project._get_credentials('http://foo', password='bar')

  @raises(AzkabanError)
  def test_missing_protocol(self):
    self.project._get_credentials('foo', password='bar')

  @raises(AzkabanError)
  def test_bad_password(self):
    self.project._get_credentials(self.url, password='bar')

  @raises(AzkabanError)
  def test_missing_archive(self):
    self.project.upload('foo', alias=self.valid_alias)

  def test_upload_simple(self):
    with temppath() as archive:
      self.project.add_job('test', Job({'type': 'noop'}))
      self.project.build(archive)
      res = self.project.upload(archive, alias=self.valid_alias)
      eq_(['projectId', 'version'], res.keys())

  @raises(AzkabanError)
  def test_upload_missing_type(self):
    with temppath() as archive:
      self.project.add_job('test', Job())
      self.project.build(archive)
      self.project.upload(archive, alias=self.valid_alias)

  def test_upload_pig_job(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      self.project.add_job('foo', PigJob(path))
      with temppath() as archive:
        self.project.build(archive)
        res = self.project.upload(archive, alias=self.valid_alias)
        eq_(['projectId', 'version'], sorted(res.keys()))

  def test_run_simple_workflow(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, alias=self.valid_alias)
    res = self.project.run('foo', alias=self.valid_alias)
    eq_(['execid', 'flow', 'message', 'project'], sorted(res.keys()))
    eq_(res['message'][:32], 'Execution submitted successfully')

  def test_run_workflow_with_dependencies(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, alias=self.valid_alias)
    res = self.project.run('bar', alias=self.valid_alias)
    eq_(['execid', 'flow', 'message', 'project'], sorted(res.keys()))
    eq_(res['message'][:32], 'Execution submitted successfully')

  @raises(AzkabanError)
  def test_run_missing_workflow(self):
    self.project.run('foo', alias=self.valid_alias)

  @raises(AzkabanError)
  def test_run_non_workflow_job(self):
    options = {'type': 'command', 'command': 'ls'}
    self.project.add_job('foo', Job(options))
    self.project.add_job('bar', Job(options, {'dependencies': 'foo'}))
    with temppath() as archive:
      self.project.build(archive)
      self.project.upload(archive, alias=self.valid_alias)
    self.project.run('foo', alias=self.valid_alias)
