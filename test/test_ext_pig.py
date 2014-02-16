#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban pig extension."""

from azkaban.ext.pig import *
from azkaban.project import Project
from azkaban.util import AzkabanError, temppath
from nose.tools import eq_, ok_, raises, nottest


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
