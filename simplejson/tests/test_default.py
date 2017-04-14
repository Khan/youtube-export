# TODO(colin): fix these lint errors (http://pep8.readthedocs.io/en/release-1.7.x/intro.html#error-codes)
# pep8-disable:E302
from unittest import TestCase

import simplejson as json

class TestDefault(TestCase):
    def test_default(self):
        self.assertEquals(
            json.dumps(type, default=repr),
            json.dumps(repr(type)))
