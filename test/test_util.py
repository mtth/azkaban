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


class TestGetSession(object):

  @raises(AzkabanError)
  def test_bad_url(self):
    get_session('http://foo', password='bar')

  @raises(AzkabanError)
  def test_missing_protocol(self):
    get_session('foo', password='bar')
