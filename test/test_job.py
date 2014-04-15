#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban job module."""

from azkaban.job import *
from azkaban.project import Project
from azkaban.util import AzkabanError, temppath
from nose.tools import eq_, ok_, raises, nottest


class TestJob(object):

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

  def test_join_options(self):
    job = Job({'bar': range(3)})
    job.join_option('bar', ',')
    eq_(job.options['bar'], '0,1,2')

  def test_join_options_with_custom_formatter(self):
    job = Job({'bar': range(3)})
    job.join_option('bar', ' ', '(%s)')
    eq_(job.options['bar'], '(0) (1) (2)')

  def test_join_missing_option(self):
    job = Job({'bar': '1'})
    job.join_option('baz', ' ', '(%s)')
    eq_(job.options['bar'], '1')
    ok_(not 'baz' in job.options)

  def test_join_prefix(self):
    job = Job({'bar': {'a': 1, 'b.c': 'foo'}})
    job.join_prefix('bar', ',', '%s-%s')
    eq_(job.options['bar'], 'a-1,b.c-foo')
