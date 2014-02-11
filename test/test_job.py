#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban job module."""

from azkaban.job import *
from azkaban.project import Project
from azkaban.util import AzkabanError, temppath
from nose.tools import eq_, ok_, raises, nottest


class TestJob(object):

  def test_options_tuple(self):
    eq_(Job().options_tuple, ())
    eq_(Job({'foo': 1}).options_tuple, ({'foo': 1}, ))
    eq_(Job({'foo': 1}, {}).options_tuple, ({'foo': 1}, {}))

  def test_option_names(self):
    eq_(Job().option_names, set())
    eq_(Job({'foo': 1}, {}).option_names, set(['foo']))
    eq_(Job({'foo': 1}, {'foo': 2}).option_names, set(['foo']))
    eq_(Job({'foo': 1}, {'bar': 2}).option_names, set(['foo', 'bar']))

  @raises(AzkabanError)
  def test_get_missing_option(self):
    Job({'foo': 1}).get_option('bar')

  def test_get_option(self):
    eq_(Job({'foo': 1}, {}).get_option('foo'), 1)
    eq_(Job({'foo': 1}, {'foo': 2}).get_option('foo'), 2)
    eq_(Job({'foo': 1}, {'bar': 2}).get_option('foo'), 1)

  def test_get_option_list(self):
    eq_(Job({}).get_option_list('foo'), [])
    eq_(Job({'foo': 1}, {}).get_option_list('bar'), [])
    eq_(Job({'foo': 1}, {}).get_option_list('foo'), [1])
    eq_(Job({'foo': 1}, {'foo': 2}).get_option_list('foo'), [1, 2])
    eq_(Job({'foo': 1}, {'foo': 2, 'bar': 3}).get_option_list('foo'), [1, 2])

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

  def test_on_add(self):
    project = Project('pj')
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('-- pig script')
      project.add_job('foo', PigJob(path))
      eq_(project._files, {path: None})
