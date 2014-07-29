#!/usr/bin/env python
# encoding: utf-8

"""Test CLI."""

from azkaban.__main__ import _parse_project, main
from azkaban.util import AzkabanError
from nose.tools import *


class TestParseProject(object):

  def test_default(self):
    eq_(_parse_project(None), ('jobs', None))

  def test_normal(self):
    eq_(_parse_project('foo'), ('foo', None))
    eq_(_parse_project('foo.py'), ('foo.py', None))
    eq_(_parse_project('foo/'), ('foo/', None))

  @raises(ImportError)
  def test_missing_module(self):
    eq_(_parse_project('foo/:bar'), ('bar', None))

  @raises(AzkabanError)
  def test_require_project(self):
    _parse_project(':bar', require_project=True)


class TestMain(object):

  pass # TODO: add test for the CLI
