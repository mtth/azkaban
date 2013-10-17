#!/usr/bin/env python
# encoding: utf-8

"""Test module."""

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


class TestProject(object):

  """TODO: TestProject docstring"""

  def test_build_single_job(self):
    """TODO: test_zip_single_job docstring."""
    project = Project('pj')
    project.add_job('foo', Job({'a': 2}))
    with temppath() as path:
      project.build(path)
      with ZipFile(path) as reader:
        ok_('foo.job' in reader.namelist())
        eq_(reader.read('foo.job'), 'a=2\n')

  def test_build_multiple_jobs(self):
    """TODO: test_zip_single_job docstring."""
    project = Project('pj')
    project.add_job('foo', Job({'a': 2}))
    project.add_job('bar', Job({'b': 3}))
    with temppath() as path:
      project.build(path)
      with ZipFile(path) as reader:
        ok_('foo.job' in reader.namelist())
        ok_('bar.job' in reader.namelist())
        eq_(reader.read('foo.job'), 'a=2\n')
