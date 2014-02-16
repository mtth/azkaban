#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban util module."""

from azkaban.util import *
from nose.tools import eq_, ok_, raises, nottest


class TestFlatten(object):

  def test_empty(self):
    eq_(flatten({}), {})

  def test_simple(self):
    dct = {'a': 1, 'B': 2}
    eq_(flatten(dct), dct)

  def test_nested(self):
    dct = {'a': 1, 'b': {'c': 3}}
    eq_(flatten(dct), {'a': 1, 'b.c': 3})


class TestConfig(object):

  @raises(AzkabanError)
  def test_missing(self):
    Config('some/inexistent/path')

  @raises(AzkabanError)
  def test_invalid_file(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('a = 1\n')
      Config(path)

  def test_parser_get(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[alias]\nfoo = 1\n')
      config = Config(path)
      eq_(config.parser.get('alias', 'foo'), '1')

  def test_save(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[alias]\nfoo = 1\n')
      config = Config(path)
      config.parser.set('alias', 'bar', 'hello')
      config.save()
      same_config = Config(path)
      eq_(same_config.parser.get('alias', 'bar'), 'hello')

  def test_get_default_option_when_exists(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[cmd]\ndefault.opt = foo\ndefault.bar = hi\n')
      config = Config(path)
      eq_(config.get_default_option('cmd', 'opt'), 'foo')

  @raises(AzkabanError)
  def test_get_default_option_when_option_is_missing(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[cmd]\ndefault.opt = foo\ndefault.bar = hi\n')
      config = Config(path)
      config.get_default_option('cmd', 'opt2')

  @raises(AzkabanError)
  def test_get_default_option_when_section_is_missing(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[cmd]\ndefault.opt = foo\ndefault.bar = hi\n')
      config = Config(path)
      config.get_default_option('cmd2', 'opt2')
