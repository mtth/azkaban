#!/usr/bin/env python
# encoding: utf-8

"""Test CLI."""

from azkaban.__main__ import _parse_project, main
from azkaban.util import AzkabanError
from contextlib import contextmanager
from nose.tools import *
from shutil import rmtree
from tempfile import mkdtemp
import imp
import os
import os.path as osp
import sys


class TestParseProject(object):

  index = 0
  # python modules are hard to reload, we go around this by making all module
  # names unique

  @contextmanager
  def _temp_project(self, module_name=None, projects=None):
    path = mkdtemp()
    _cwd = os.getcwd()
    try:
      os.chdir(path)
      if module_name:
        module_path = osp.join(path, '%s_%s.py' % (module_name, self.index))
        self.index += 1
        with open(module_path, 'w') as writer:
          # yay for code generation, oh scala
          writer.write('from azkaban import Project\n')
          for index, (name, register) in enumerate(projects or []):
            writer.write(
              'pj_%s = Project(%r, register=%r)\n'
              % (index, name, register)
            )
      else:
        module_path = None
      yield module_name
    finally:
      os.chdir(_cwd)
      rmtree(path)

  # def test_single_default(self):
  #   with _project('jobs', [('bar', True)]):
  #     name, project = _parse_project(None)
  #     eq_(name, 'bar')
  #     ok_(project is not None)

  # @raises(AzkabanError)
  # def test_missing_module_default(self):
  #   with _project():
  #     name, project = _parse_project(None)

  # @raises(AzkabanError)
  # def test_missing_project_default(self):
  #   with _project('jobs'):
  #     name, project = _parse_project(None)

  # @raises(AzkabanError)
  # def test_multiple_names_default(self):
  #   with _project('jobs', [('bar', True), ('foo', True)]):
  #     name, project = _parse_project(None)

  # def test_multiple_names_with_name(self):
  #   with _project('jobs', [('bar', True), ('foo', True)]):
  #     name, project = _parse_project('bar')
  #     eq_(name, 'bar')
  #     ok_(project is not None)

  # @raises(AzkabanError)
  # def test_multiple_names_with_missing_name(self):
  #   with _project('jobs', [('bar', True), ('foo', True)]):
  #     name, project = _parse_project('faa', True)

  # def test_multiple_names_with_missing_name(self):
  #   with _project('jobs', [('bar', True), ('foo', True)]):
  #     name, project = _parse_project('faa', False)
  #     eq_(name, 'faa')
  #     ok_(project is None)

  # def test_normal(self):
  #   eq_(_parse_project('foo'), ('foo', None))
  #   eq_(_parse_project('foo.py'), ('foo.py', None))
  #   eq_(_parse_project('foo/'), ('foo/', None))

  # @raises(ImportError)
  # def test_missing_module(self):
  #   eq_(_parse_project('foo/:bar'), ('bar', None))

  # @raises(AzkabanError)
  # def test_require_project(self):
  #   _parse_project(':bar', require_project=True)


class TestMain(object):

  pass # TODO: add test for the CLI
