#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban project module."""

from azkaban.project import *
from azkaban.job import Job
from azkaban.util import AzkabanError, flatten, temppath
from ConfigParser import RawConfigParser
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest
from os.path import expanduser, relpath, abspath, join
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
