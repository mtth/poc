import re

class Parser(object):
    """
    Parses a pig file into a PigLine objects
    """

    outputs_pattern = re.compile('((?P<output>\w+)\s*=)?\s*(?P<operator>\w+)')

    def _outputs(self, line):
        matched = self.outputs_pattern.match(line).groupdict()
        if matched['operator'].upper() == 'SPLIT':
            return {'operator': 'SPLIT',
                    'outputs': set(self._inputs('_SPLIT', line))}
        else:
            return {'operator': operator,
                    'outputs': set([matched['output']])}

    def _inputs(self, operator, line):
        split = self.operator_splits[operator]
        if split:
            pattern = re.compile('(?P<alias>\w+)\s*(%s)' % (split, ), re.I)
            return [match.groupdict()['alias'] for match in pattern.finditer(line)]
        else:
            pattern = re.compile('%s\s+(?P<alias>)' % (operator, ), re.I)
            return pattern.search(line).groupdict['alias']

    operator_splits = {
        'DISTINCT': None,
        'FILTER': None,
        'FOREACH': None,
        'LIMIT': None,
        'LOAD': None,
        'ORDER': None,
        'RANK': None,
        'SAMPLE': None,
        'ASSERT': None,
        'DUMP': None,
        'STORE': None,
        'JOIN': 'BY',
        'GROUP': 'BY',
        'COGROUP': 'BY',
        'STREAM': ',',
        'UNION': ',',
        'SPLIT': None,
        '_SPLIT': 'IF|OTHERWISE',
    }

    def parse_line(line):
        outputs = self._outputs(line)
        matches = {
            'inputs': set(self._inputs(outputs['operator'], line)),
            'outputs': outputs['outputs'],
            'operator': outputs['operator'],
        }
        return matches

    def parse_file(path):
        with open(path) as handle:
            return [self.parse_line(line.strip()) for line in handle.read().split(';')]


class Operator(object):
    """
    Holds Operator name, input and output set of alias strings.
    """

    def __init__(self, name, input_set=frozenset(), output_set=frozenset()):
        self.name = name
        self.input_set = input_set
        self.output_set = output_set

class Flow(object):
    pass

"""
ALL CASE-INSENSITIVE:

<relation> = OP <alias>;
    - DISTINCT
    - FILTER
    - FOREACH
    - LIMIT
    - ORDER
    - RANK
    - SAMPLE
    - LOAD

OP <alias>;
    - DUMP
    - ASSERT
    - STORE

<relation> = (JOIN|GROUP) [<alias> BY, ...];

<relation> = (STREAM|UNION) [<alias>, ...];
    - UNION can have [ONSCHEMA] before a relation

SPLIT <alias> INTO [<relation> (IF|OTHERWISE), ...]
"""
