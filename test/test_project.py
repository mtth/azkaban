#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban project module."""

from azkaban.project import *
from azkaban.job import Job
from azkaban.util import AzkabanError, flatten, temppath
from nose.tools import eq_, ok_, raises, nottest
from nose.plugins.skip import SkipTest
from os import pardir
from os.path import basename, dirname, expanduser, relpath, abspath, join
from requests import ConnectionError, post
from six.moves.configparser import RawConfigParser
from time import sleep, time
from zipfile import ZipFile


# filepaths for testing
FILEPATHS = [__file__, abspath(join(dirname(__file__), pardir, 'README.md'))]


class _TestProject(object):

  def setup(self):
    self.project = Project('foo')


class TestProjectAddFile(_TestProject):

  def test_add_file(self):
    self.project.add_file(__file__, 'bar')
    eq_(self.project._files, {'bar': (__file__, True)})

  def test_add_relative_file(self):
    project = Project('foo', root=__file__)
    project.add_file('test_job.py')
    path = join(dirname(__file__), 'test_job.py')
    eq_(project._files, {'test_job.py': (path, False)})

  def test_add_relative_file_with_archive_path(self):
    project = Project('foo', root=__file__)
    project.add_file('test_job.py', 'bar')
    eq_(
      project._files,
      {'bar': (join(dirname(__file__), 'test_job.py'), True)}
    )

  @raises(AzkabanError)
  def test_add_relative_file_outside_root(self):
    project = Project('foo', root=__file__)
    project.add_file(FILEPATHS[1])

  @raises(AzkabanError)
  def test_missing_file(self):
    self.project.add_file('bar')

  @raises(AzkabanError)
  def test_add_relative_file_without_root(self):
    self.project.add_file(relpath(__file__))

  def test_add_duplicate_file(self):
    self.project.add_file(__file__)
    self.project.add_file(__file__)
    eq_(self.project._files, {__file__.lstrip('/'): (__file__, False)})

  def test_add_duplicate_file_with_archive_path(self):
    self.project.add_file(FILEPATHS[0], 'foo')
    self.project.add_file(FILEPATHS[0], 'foo')
    eq_(self.project._files, {'foo': (__file__, True)})

  @raises(AzkabanError)
  def test_add_inconsistent_duplicate_file(self):
    self.project.add_file(FILEPATHS[0], 'foo')
    self.project.add_file(FILEPATHS[1], 'foo')

  def test_add_inconsistent_duplicate_file_with_overwrite(self):
    self.project.add_file(FILEPATHS[0], 'foo')
    self.project.add_file(FILEPATHS[1], 'foo', overwrite=True)
    path = abspath(FILEPATHS[1])
    eq_(self.project._files, {'foo': (path, True)})

  def test_files(self):
    self.project.add_file(FILEPATHS[0], 'foo')
    self.project.add_file(FILEPATHS[1])
    files = [(FILEPATHS[1], FILEPATHS[1].lstrip('/')), (FILEPATHS[0], 'foo')]
    eq_(sorted(self.project.files), files)


class TestProjectAddJob(_TestProject):

  def test_add_job(self):
    class OtherJob(Job):
      test = None
      def on_add(self, project, name, **kwargs):
        self.test = (project.name, name, kwargs)
    job = OtherJob()
    self.project.add_job('bar', job, baz=48)
    eq_(job.test, ('foo', 'bar', {'baz': 48}))
    ok_('bar' in self.project.jobs)

  def test_add_duplicate_consistent_job(self):
    job = Job()
    self.project.add_job('bar', job)
    self.project.add_job('bar', job)

  @raises(AzkabanError)
  def test_add_duplicate_inconsistent_job(self):
    self.project.add_job('bar', Job())
    self.project.add_job('bar', Job())

  def test_job(self):
    job = Job()
    self.project.add_job('bar', job)
    eq_(self.project.jobs['bar'], job)

  @raises(AzkabanError)
  def test_add_job_to_property(self):
    job = Job()
    self.project.jobs['bar'] = job

  @raises(AzkabanError)
  def test_missing_job(self):
    self.project.jobs['bar']


class TestProjectMerge(_TestProject):

  def test_merge_project(self):
    job_bar = Job()
    self.project.add_job('bar', job_bar)
    self.project.add_file(FILEPATHS[0], 'bar')
    project2 = Project('qux')
    job_baz = Job()
    project2.add_job('baz', job_baz)
    project2.add_file(FILEPATHS[1], 'baz')
    project2.merge_into(self.project)
    eq_(self.project.name, 'foo')
    eq_(self.project._jobs, {'bar': job_bar, 'baz': job_baz})
    eq_(
      self.project._files,
      {'bar': (FILEPATHS[0], True), 'baz': (FILEPATHS[1], True)}
    )

  def test_merge_project_on_add_kwargs(self):
    class OtherJob(Job):
      test = None
      def on_add(self, project, name, **kwargs):
        self.test = kwargs
    job = OtherJob()
    project2 = Project('qux')
    project2.add_job('bar', job)
    project2.merge_into(self.project)
    eq_(job.test, {'merging': project2})

  def test_merge_project_with_different_roots(self):
    project2 = Project('qux', root=__file__)
    project2.add_file(__file__)
    project2.add_file(__file__, 'bar')
    project1 = Project('oth', root=dirname(dirname(abspath(__file__))))
    project2.merge_into(project1)
    eq_(sorted(project1._files), ['bar', join('test', basename(__file__))])


class TestProjectBuild(_TestProject):

  @raises(AzkabanError)
  def test_build_empty(self):
    with temppath() as path:
      self.project.build(path)

  def test_build_single_job(self):
    job = Job({'a': 2})
    self.project.add_job('bar', job)
    with temppath() as path:
      self.project.build(path)
      reader =  ZipFile(path)
      try:
        ok_('bar.job' in reader.namelist())
        eq_(reader.read('bar.job').decode('utf-8'), 'a=2\n')
      finally:
        reader.close()

  def test_build_with_file(self):
    self.project.add_file(__file__.rstrip('c'), 'this.py')
    with temppath() as path:
      self.project.build(path)
      reader = ZipFile(path)
      try:
        ok_('this.py' in reader.namelist())
        eq_(
          reader.read('this.py').decode('utf-8').split('\n')[0],
          '#!/usr/bin/env python'
        )
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
        eq_(reader.read('foo.job').decode('utf-8'), 'a=2\n')
      finally:
        reader.close()


class TestProjectProperties(_TestProject):

  def test_no_properties_by_default(self):
    self.project.add_job('foo', Job({'a': 2}))
    with temppath() as path:
      self.project.build(path)
      reader = ZipFile(path)
      try:
        eq_(reader.namelist(), ['foo.job'])
      finally:
        reader.close()

  def test_properties_if_defined(self):
    self.project.add_job('foo', Job({'a': 2}))
    self.project.properties = {'bar': 123}
    with temppath() as path:
      self.project.build(path)
      reader = ZipFile(path)
      try:
        eq_(sorted(reader.namelist()), ['foo.job', 'project.properties'])
        eq_(
          reader.read('project.properties').decode('utf-8'),
          'bar=123\n'
        )
      finally:
        reader.close()

  def test_properties_flattened(self):
    self.project.add_job('foo', Job({'a': 2}))
    self.project.properties = {'param': {'foo': 1, 'bar': 'baz'}}
    with temppath() as path:
      self.project.build(path)
      reader = ZipFile(path)
      try:
        eq_(sorted(reader.namelist()), ['foo.job', 'project.properties'])
        eq_(
          reader.read('project.properties').decode('utf-8'),
          'param.bar=baz\nparam.foo=1\n'
        )
      finally:
        reader.close()
