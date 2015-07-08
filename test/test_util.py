#!/usr/bin/env python
# encoding: utf-8

"""Test Azkaban util module."""

from azkaban.util import *
from contextlib import contextmanager
from nose.tools import eq_, ok_, raises, nottest
from six import u


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
  def test_invalid_file(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('a = 1\n')
      Config(path)

  def test_parser_get(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[foo]\nbar = 1\n')
      config = Config(path)
      eq_(config.parser.get('foo', 'bar'), '1')

  def test_save(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[foo]\nbar = 1\n')
      config = Config(path)
      config.parser.set('foo', 'bar', 'hello')
      config.save()
      same_config = Config(path)
      eq_(same_config.parser.get('foo', 'bar'), 'hello')

  def test_get_default_option_when_exists(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[cmd]\nopt = foo\nbar = hi\n')
      config = Config(path)
      eq_(config.get_option('cmd', 'opt'), 'foo')

  @raises(AzkabanError)
  def test_get_default_option_when_option_is_missing(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[cmd]\nopt = foo\nbar = hi\n')
      config = Config(path)
      config.get_option('cmd', 'opt2')

  @raises(AzkabanError)
  def test_get_default_option_when_section_is_missing(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[cmd]\nopt = foo\nbar = hi\n')
      config = Config(path)
      config.get_option('cmd2', 'opt2')

  def test_convert_aliases(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('[alias]\nfoo = 1\nbar = hello')
      config = Config(path)
      ok_(not config.parser.has_section('alias'))
      eq_(config.parser.get('alias.foo', 'url'), '1')
      ok_(not config.parser.getboolean('alias.foo', 'verify'))
      eq_(config.parser.get('alias.bar', 'url'), 'hello')


class TestMultipartForm(object):

  def get_form_content(self, form):
    return b''.join(chunk for chunk in form)

  def test_single_file(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('HAI')
      form = MultipartForm([
        {'path': path, 'name': 'foo', 'type': 'text/plain'}
      ])
      ok_(b'HAI' in self.get_form_content(form))

  def test_multiple_files(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('HAI')
      form = MultipartForm([
        {'path': path, 'name': 'foo', 'type': 'text/plain'},
        path,
      ])
      ok_(b'HAI' in u(self.get_form_content(form)))

  def test_params(self):
    with temppath() as path:
      with open(path, 'w') as writer:
        writer.write('HAI')
      form = MultipartForm(
        files=[{'path': path, 'name': 'foo', 'type': 'text/plain'}, path],
        params={'foo': 'bar'},
      )
      content = self.get_form_content(form)
      ok_(b'name="foo"' in content)
      ok_(b'bar' in content)


class TestReadProperties(object):

  @staticmethod
  @contextmanager
  def temp_properties(contents):
    with temppath() as tpath:
      with open(tpath, 'w') as writer:
        writer.write(contents)
      yield tpath

  def test_separators(self):
    contents = 'a=1\nb=2\nc  4'
    with self.temp_properties(contents) as path:
      eq_(read_properties(path), {'a': '1', 'b': '2', 'c': '4'})

  def test_comments(self):
    contents = '# fee\na=1\nb=2\n   !faye\n'
    with self.temp_properties(contents) as path:
      eq_(read_properties(path), {'a': '1', 'b': '2'})

  def test_escaped_newline(self):
    contents = 'a = bo\\\n    hi\nb = 2\n'
    with self.temp_properties(contents) as path:
      eq_(read_properties(path), {'a': 'bohi', 'b': '2'})

  def test_empty_value(self):
    contents = 'a = b\nc\nd : 4'
    with self.temp_properties(contents) as path:
      eq_(read_properties(path), {'a': 'b', 'c': '', 'd': '4'})

  def test_escaped_separators(self):
    contents = 'a\=b = 5\nfoo\ bar :ja\n'
    with self.temp_properties(contents) as path:
      eq_(read_properties(path), {'a=b': '5', 'foo bar': 'ja'})
