#!/usr/bin/env python
# encoding: utf-8

"""Test CLI."""

from azkaban.__main__ import _parse_project, main
from nose.tools import *


class TestParseProject(object):

  def test_default(self):
    eq_(_parse_project(None), ('jobs', '.', None))

  def test_module(self):
    eq_(_parse_project('foo'), ('foo', '.', None))
    eq_(_parse_project('foo.py'), ('foo', '.', None))
    eq_(_parse_project('foo/'), ('foo', '.', None))
    eq_(_parse_project('foo/:bar'), ('foo', '.', 'bar'))
    eq_(_parse_project('/hi/foo/:bar'), ('foo', '/hi', 'bar'))

  def test_name(self):
    eq_(_parse_project('foo:bar'), ('foo', '.', 'bar'))
    eq_(_parse_project(':bar'), ('jobs', '.', 'bar'))

  def test_path(self):
    eq_(_parse_project('/hi/foo'), ('foo', '/hi', None))
    eq_(_parse_project('/hi/foo:bar'), ('foo', '/hi', 'bar'))


class TestMain(object):

  pass # TODO
