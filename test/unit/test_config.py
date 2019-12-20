import unittest
from unittest.mock import patch

import yaml

from druzhba.config import _parse_config

NO_SUB = """---
one: foo
dict:
  one: two
  three: four
list:
  - a: b
    b: c
  - a: d
    b: e
"""

SUB_SHALLOW = """---
one: ${FOO}
dict:
  one: two
  three: four
list:
  - a: b
    b: c
  - a: d
    b: e
"""

SUB_DEEP = """---
one: foo
dict:
  one: two
  three: four
list:
  - a: b
    b: ${FOO}
  - a: d
    b: e
"""

SUB_MULTI = """---
one: foo
dict:
  one: " ${FOO}-{$}Xx${BAR}"
  three: four
list:
  - a: b
    b: ${FOO}
  - a: d
    b: e
"""

SUB_WITH_UNKNOWN = """---
one: two
buckle: my ${FOO}
three: 4
shut:
  the: 3.14
"""


class TestConfigParsing(unittest.TestCase):
    def test_no_substitution(self):
        conf, _ = _parse_config(yaml.safe_load(NO_SUB))
        self.assertDictEqual(
            conf,
            {
                "dict": {"one": "two", "three": "four"},
                "list": [{"a": "b", "b": "c"}, {"a": "d", "b": "e"}],
                "one": "foo",
            },
        )

    @patch.dict("os.environ", {"FOO": ":foo:"})
    def test_shallow_substitution(self):
        conf, _ = _parse_config(yaml.safe_load(SUB_SHALLOW))
        self.assertDictEqual(
            conf,
            {
                "dict": {"one": "two", "three": "four"},
                "list": [{"a": "b", "b": "c"}, {"a": "d", "b": "e"}],
                "one": ":foo:",
            },
        )

    @patch.dict("os.environ", {"FOO": ":foo:"})
    def test_deep_substitution(self):
        conf, _ = _parse_config(yaml.safe_load(SUB_DEEP))
        self.assertDictEqual(
            conf,
            {
                "dict": {"one": "two", "three": "four"},
                "list": [{"a": "b", "b": ":foo:"}, {"a": "d", "b": "e"}],
                "one": "foo",
            },
        )

    @patch.dict("os.environ", {"FOO": ":foo:", "BAR": ":bar:"})
    def test_multi_substitution(self):
        conf, _ = _parse_config(yaml.safe_load(SUB_MULTI))
        self.assertDictEqual(
            conf,
            {
                "dict": {"one": " :foo:-{$}Xx:bar:", "three": "four"},
                "list": [{"a": "b", "b": ":foo:"}, {"a": "d", "b": "e"}],
                "one": "foo",
            },
        )

    def test_missing_all(self):
        conf, missing = _parse_config(yaml.safe_load(SUB_MULTI))
        self.assertDictEqual(
            conf,
            {
                "dict": {"one": " -{$}Xx", "three": "four"},
                "list": [{"a": "b", "b": ""}, {"a": "d", "b": "e"}],
                "one": "foo",
            },
        )
        self.assertSetEqual(missing, {"BAR", "FOO"})

    @patch.dict("os.environ", {"FOO": ":foo:"})
    def test_missing_shallow(self):
        conf, missing = _parse_config(yaml.safe_load(SUB_MULTI))
        self.assertDictEqual(
            conf,
            {
                "dict": {"one": " :foo:-{$}Xx", "three": "four"},
                "list": [{"a": "b", "b": ":foo:"}, {"a": "d", "b": "e"}],
                "one": "foo",
            },
        )
        self.assertSetEqual(missing, {"BAR"})

    @patch.dict("os.environ", {"BAR": ":bar:"})
    def test_missing_deep(self):
        conf, missing = _parse_config(yaml.safe_load(SUB_MULTI))
        self.assertDictEqual(
            conf,
            {
                "dict": {"one": " -{$}Xx:bar:", "three": "four"},
                "list": [{"a": "b", "b": ""}, {"a": "d", "b": "e"}],
                "one": "foo",
            },
        )
        self.assertSetEqual(missing, {"FOO"})

    @patch.dict("os.environ", {"FOO": ":foo:", "BAR": ":bar:"})
    def test_missing_none(self):
        conf, missing = _parse_config(yaml.safe_load(SUB_MULTI))
        self.assertDictEqual(
            conf,
            {
                "dict": {"one": " :foo:-{$}Xx:bar:", "three": "four"},
                "list": [{"a": "b", "b": ":foo:"}, {"a": "d", "b": "e"}],
                "one": "foo",
            },
        )
        self.assertSetEqual(missing, set())

    @patch.dict("os.environ", {"FOO": ":foo:"})
    def test_unknown_types(self):
        conf, missing = _parse_config(yaml.safe_load(SUB_WITH_UNKNOWN))
        self.assertDictEqual(
            conf,
            {"one": "two", "buckle": "my :foo:", "three": 4, "shut": {"the": 3.14}},
        )
        self.assertSetEqual(missing, set())
