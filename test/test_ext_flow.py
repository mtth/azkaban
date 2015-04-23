#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban pig extension.

Note the type `'pig'` enforcement to override a potential configuration option.

"""

from azkaban.ext.flow import *
from azkaban.util import read_properties, temppath
from nose.tools import eq_


class TestFlowProperty(object):

  def setup(self):
    FlowProperty.reset()

  def test_init(self):
    p1 = FlowProperty(mode1='a', mode2='b')
    p2 = FlowProperty(mode1='A', mode3='C')
    eq_(p1.key, 'flow.property.0')
    eq_(p2.key, 'flow.property.1')

  def test_str(self):
    job = Job({'foo': {'bar': FlowProperty(mode1='a', mode2='b')}})
    with temppath() as tpath:
      job.build(path=tpath)
      eq_(read_properties(tpath), {'foo.bar': '${flow.property.0}'})

  def test_get_options(self):
    p1 = FlowProperty(mode1='a', mode2='b')
    p2 = FlowProperty(mode1='A', mode3='C')
    eq_(FlowProperty.get_options('mode1'), {p1.key: 'a', p2.key: 'A'})
    eq_(FlowProperty.get_options('mode3'), {p2.key: 'C'})
    eq_(FlowProperty.get_options('mode4'), {})


class TestFlowJob(object):

  def setup(self):
    FlowProperty.reset()

  def test_options(self):
    job = FlowJob('foo', 'mode1', {'type': 'noop', 'bar': 2, 'flow.name': ''})
    eq_(job.options, {'type': 'flow', 'flow.name': 'foo', 'bar': 2})

  def test_build(self):
    p1 = FlowProperty(mode1='a', mode2='b')
    p2 = FlowProperty(mode1='A', mode3='C')
    job = FlowJob('foo', 'mode1')
    with temppath() as tpath:
      job.build(path=tpath)
      eq_(read_properties(tpath), {
        'type': 'flow',
        'flow.name': 'foo',
        'flow.property.0': 'a',
        'flow.property.1': 'A',
      })
