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


from nose.tools import eq_
from poc.flow import *


class TestFlow(object):

  path = 'poc/test/a.pig'

  def setup(self):
    self.flow = Flow(self.path)

  def test_names(self):
    eq_(
      [operator.name for operator in self.flow],
      ['LOAD', 'FOREACH', 'GROUP', 'FOREACH', 'STORE']
    )

  def test_parents(self):
    ops = self.flow.operators
    eq_(ops[0].parents, {})
    eq_(ops[1].parents, {'A': ops[0]})

  def test_children(self):
    ops = self.flow.operators
    eq_(ops[4].children, {})
    eq_(ops[0].children, {'A': ops[1]})
    eq_(
      [len(op.children) for op in ops],
      [1, 1, 1, 1, 0],
    )