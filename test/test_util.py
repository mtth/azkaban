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


class TestTabularize(object):

  @raises(ValueError)
  def test_empty_msg(self):
    data = []
    with temppath() as path:
      with open(path, 'w') as writer:
        tabularize(data, ['a'], writer=writer)

  def test_single_field(self):
    data = [{'a': 21, 'b': 3}, {'a': 4, 'b': 5}]
    with temppath() as path:
      with open(path, 'w') as writer:
        tabularize(data, ['a'], writer=writer)
      eq_(open(path).read(), '  a\n 21\n  4\n')

  def test_multiple_fields(self):
    data = [{'a': 21, 'bc': 3}, {'a': 4, 'bc': 5}]
    with temppath() as path:
      with open(path, 'w') as writer:
        tabularize(data, ['bc', 'a'], writer=writer)
      eq_(open(path).read(), ' bc  a\n  3 21\n  5  4\n')

  def test_empty_fields(self):
    data = [{'a': 21, 'bc': 3}, {'a': 4, 'b': 5}]
    with temppath() as path:
      with open(path, 'w') as writer:
        tabularize(data, ['bc', 'a'], writer=writer)
      eq_(open(path).read(), ' bc  a\n  3 21\n     4\n')

class TestGetSession(object):

  @raises(AzkabanError)
  def test_missing_alias(self):
    get_session('foo', alias='bar')

  @raises(AzkabanError)
  def test_bad_url(self):
    get_session('http://foo', password='bar')

  @raises(AzkabanError)
  def test_missing_protocol(self):
    get_session('foo', password='bar')
