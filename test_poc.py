#!/usr/bin/env python
# encoding: utf-8

"""Poc test module.

Pig grammar:

alias = CROSS alias, alias [, alias ...] [PARTITION BY partitioner] [PARALLEL n];
alias = CUBE alias BY { CUBE expression | ROLLUP expression }, [ CUBE expression | ROLLUP expression ] [PARALLEL n];
alias = DISTINCT alias [PARTITION BY partitioner] [PARALLEL n];
alias = FILTER alias BY expression;
alias = FOREACH alias GENERATE expression [AS schema] [expression [AS schema] ...];
alias = GROUP alias { ALL | BY expression}[, alias ALL | BY expression ...]  [USING 'collected' | 'merge'] [PARTITION BY partitioner] [PARALLEL n];
alias = JOIN alias BY {expression|'('expression [, expression ...]')'} (, alias BY {expression|'('expression [, expression ...]')'} ...) [USING 'replicated' | 'skewed' | 'merge' | 'merge-sparse'] [PARTITION BY partitioner] [PARALLEL n];
alias = JOIN left-alias BY left-alias-column [LEFT|RIGHT|FULL] [OUTER], right-alias BY right-alias-column [USING 'replicated' | 'skewed' | 'merge'] [PARTITION BY partitioner] [PARALLEL n];
alias = LIMIT alias n;
alias1 = MAPREDUCE 'mr.jar' STORE alias2 INTO 'inputLocation' USING storeFunc LOAD 'outputLocation' USING loadFunc AS schema [`params, ... `];
alias = ORDER alias BY { * [ASC|DESC] | field_alias [ASC|DESC] [, field_alias [ASC|DESC] ...] } [PARALLEL n];
alias = RANK alias [ BY { * [ASC|DESC] | field_alias [ASC|DESC] [, field_alias [ASC|DESC] ...] } [DENSE] ];
alias = STREAM alias [, alias ...] THROUGH {`command` | cmd_alias }[AS schema];
alias = UNION [ONSCHEMA] alias, alias [, alias ...];
alias = LOAD 'data' [USING function] [AS schema];
alias = SAMPLE alias size;
SPLIT alias INTO alias IF expression, alias IF expression [, alias IF expression ...] [, alias OTHERWISE];
STORE alias INTO 'directory' [USING function];
REGISTER path;
DUMP alias;
ASSERT alias BY expression [message];

"""


from poc import *
from nose.tools import eq_


class TestParser(object):

  def setup(self):
    self.parser = Parser()

  def test_store(self):
    matched = self.parser.parse_line("stOrE foo into 'some/path'")
    truth = {
      'operator': 'STORE',
      'outputs': set(),
      'inputs': set(['foo']),
    }
    eq_(matched, truth)

  def test_distinct(self):
    matched = self.parser.parse_line('foo = DISTINCT bar')
    truth = {
      'operator': 'DISTINCT',
      'outputs': set(['foo']),
      'inputs': set(['bar']),
    }
    eq_(matched, truth)

  def test_filter(self):
    matched = self.parser.parse_line('foo = filter bar by baz')
    truth = {
      'operator': 'FILTER',
      'outputs': set(['foo']),
      'inputs': set(['bar']),
    }
    eq_(matched, truth)

  def test_join(self):
    matched = self.parser.parse_line('foo = join this by fi, that by fie')
    truth = {
      'operator': 'JOIN',
      'outputs': set(['foo']),
      'inputs': set(['this', 'that']),
    }
    eq_(matched, truth)

  def test_split_normal(self):
    matched = self.parser.parse_line('split foo into bar if this, bax if that')
    truth = {
      'operator': 'SPLIT',
      'outputs': set(['bar', 'bax']),
      'inputs': set(['foo']),
    }
    eq_(matched, truth)

  def test_split_otherwise(self):
    matched = self.parser.parse_line('split foo into bar if 1, bax otherwise')
    truth = {
      'operator': 'SPLIT',
      'outputs': set(['bar', 'bax']),
      'inputs': set(['foo']),
    }
    eq_(matched, truth)
