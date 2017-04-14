# TODO(colin): fix these lint errors (http://pep8.readthedocs.io/en/release-1.7.x/intro.html#error-codes)
# pep8-disable:E302
from unittest import TestCase
import simplejson as json

# from http://json.org/JSON_checker/test/pass2.json
JSON = r'''
[[[[[[[[[[[[[[[[[[["Not too deep"]]]]]]]]]]]]]]]]]]]
'''

class TestPass2(TestCase):
    def test_parse(self):
        # test in/out equivalence and parsing
        res = json.loads(JSON)
        out = json.dumps(res)
        self.assertEquals(res, json.loads(out))
