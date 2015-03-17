#!/usr/bin/python

from lib.rome.core.orm.query import Query
from _fixtures import *

import unittest


class TestSelection(unittest.TestCase):
    def test_selection(self):
        compute_nodes = Query(ComputeNode).all()
        print(compute_nodes)
        self.assertEqual(True, True)


if __name__ == '__main__':
    erase_fixture_data()
    initialise_fixture_data()
    unittest.main()