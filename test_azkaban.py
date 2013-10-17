#!/usr/bin/env python
# encoding: utf-8

"""Azkaban test module."""

from nose.tools import eq_, ok_, raises, nottest

from azkaban import *


class TestFlatten(object):

  """TODO: TestFlatten docstring"""

  def test_empty(self):
    """TODO: test_empty docstring."""
    eq_(flatten({}), {})

  def test_simple(self):
    """TODO: test_simple docstring."""
    dct = {'a': 1, 'B': 2}
    eq_(flatten(dct), dct)

  def test_nested(self):
    """TODO: test_nested docstring."""
    dct = {'a': 1, 'b': {'c': 3}}
    eq_(flatten(dct), {'a': 1, 'b.c': 3})


class TestProject(object):

  """TODO: TestProject docstring"""

  def test_add_file(self):
    """TODO: test_add_file docstring."""
    project = Project('foo')
    project.add_file(__file__, 'bar')
    eq_(project._files, {__file__: 'bar'})

  @raises(AzkabanError)
  def test_missing_file(self):
    """TODO: test_missing_file docstring."""
    project = Project('foo')
    project.add_file('bar')

  def test_add_duplicate_file(self):
    """TODO: test_add_duplicate_file docstring."""
    project = Project('foo')
    project.add_file(__file__)
    project.add_file(__file__)
    eq_(project._files, {__file__: None})

  @raises(AzkabanError)
  def test_add_inconsistent_duplicate_file(self):
    """TODO: test_add_inconsistent_duplicate_file docstring."""
    project = Project('foo')
    project.add_file(__file__)
    project.add_file(__file__, 'this.py')

  def test_add_job(self):
    """TODO: test_add_job docstring."""
    project = Project('foo')
    class OtherJob(Job):
      test = None
      def on_add(self, project, name):
        self.test = (project.name, name)
    job = OtherJob()
    project.add_job('bar', job)
    eq_(job.test, ('foo', 'bar'))

  @raises(AzkabanError)
  def test_add_duplicate_job(self):
    """TODO: test_add_duplicate_job docstring."""
    project = Project('foo')
    project.add_job('bar', Job())
    project.add_job('bar', Job())

  def test_build_single_job(self):
    """TODO: test_zip_single_job docstring."""
    project = Project('foo')
    class OtherJob(Job):
      test = None
      def on_build(self, project, name):
        self.test = (project.name, name)
    job = OtherJob({'a': 2})
    project.add_job('bar', job)
    with temppath() as path:
      project.build(path)
      eq_(job.test, ('foo', 'bar'))
      with ZipFile(path) as reader:
        ok_('bar.job' in reader.namelist())
        eq_(reader.read('bar.job'), 'a=2\n')

  def test_build_with_file(self):
    """TODO: test_build_with_file docstring."""
    project = Project('pf')
    project.add_file(__file__, 'this.py')
    with temppath() as path:
      project.build(path)
      with ZipFile(path) as reader:
        ok_('this.py' in reader.namelist())
        eq_(reader.read('this.py').split('\n')[7], 'from azkaban import *')

  def test_build_multiple_jobs(self):
    """TODO: test_zip_single_job docstring."""
    project = Project('pj')
    project.add_job('foo', Job({'a': 2}))
    project.add_job('bar', Job({'b': 3}))
    project.add_file(__file__, 'this.py')
    with temppath() as path:
      project.build(path)
      with ZipFile(path) as reader:
        ok_('foo.job' in reader.namelist())
        ok_('bar.job' in reader.namelist())
        ok_('this.py' in reader.namelist())
        eq_(reader.read('foo.job'), 'a=2\n')


class TestJob(object):

  """TODO: TestJob docstring"""

  def test_generate_simple(self):
    """TODO: test_generate docstring."""
    job = Job({'a': 1, 'b': {'c': 2, 'd': 3}})
    with temppath() as path:
      job.generate(path)
      with open(path) as reader:
        eq_(reader.read(), 'a=1\nb.c=2\nb.d=3\n')

  def test_generate_with_defaults(self):
    """TODO: test_generate_with_defaults docstring."""
    defaults = {'b': {'d': 4}, 'e': 5}
    job = Job({'a': 1, 'b': {'c': 2, 'd': 3}}, defaults)
    with temppath() as path:
      job.generate(path)
      with open(path) as reader:
        eq_(reader.read(), 'a=1\nb.c=2\nb.d=3\ne=5\n')

  def test_generate_with_dependencies(self):
    """TODO: test_generate_with_dependencies docstring."""
    foo = Job()
    bar = Job({'a': 3})
    job = Job({'a': 2, 'dependencies': 'bar,foo'})
    with temppath() as path:
      job.generate(path)
      with open(path) as reader:
        eq_(reader.read(), 'a=2\ndependencies=bar,foo\n')


class TestPigJob(object):

  """TODO: TestPigJob docstring"""

  def test_init(self):
    """TODO: test_init docstring."""
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      job = PigJob(path, {'a': 2}, {'a': 3, 'b': 4}, {'type': 'noop'})
      with temppath() as tpath:
        job.generate(tpath)
        with open(tpath) as reader:
          eq_(reader.read(), 'a=2\nb=4\npig.script=%s\ntype=pig\n' % (path, ))

  def test_type(self):
    """TODO: test_type docstring."""
    class OtherPigJob(PigJob):
      type = 'pigfoo'
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      job = OtherPigJob(path, {'type': 'pigbar'})
      with temppath() as tpath:
        job.generate(tpath)
        with open(tpath) as reader:
          eq_(reader.read(), 'pig.script=%s\ntype=pigfoo\n' % (path, ))

  @raises(AzkabanError)
  def test_missing(self):
    """TODO: test_missing docstring."""
    PigJob('foo.pig')

  def test_on_add(self):
    """TODO: test_on_add docstring."""
    project = Project('pj')
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      project.add_job('foo', PigJob(path))
      eq_(project._files, {path: None})
