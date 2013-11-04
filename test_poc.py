from poc import *
from nose.tools import eq_ 

class TestInitial(object):

    def setup(self):
        self.parser = Parser()

    def test_unary(self):
       matched = self.parser.parse_line("stOrE foo into 'some/path'")
       eq_(matched['output'], None)
       eq_(matched['operator'], 'stOrE')

    def test_binary(self):
       matched = Parser.initial.match("foo = DISTINCT bar").groupdict()
       eq_(matched['relation'], 'foo')
       eq_(matched['operator'], 'DISTINCT')
