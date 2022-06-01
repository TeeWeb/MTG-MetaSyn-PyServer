from types import resolve_bases
from typing import Dict
import unittest

from pip import List

from app.dbmanager import Manager

class TestLocalDataFormat(unittest.TestCase):

    def __init__(self):
        self.mgr = Manager()

    def test_get_cards_local_data(self):
        assert(type(self.mgr.cards.local.get_data()) is List)

    def test_get_types_local_data(self):
        assert(type(self.mgr.types.local.get_data()) is List)

    def test_get_keywords_local_data(self):
        assert(type(self.mgr.keywords.local.get_data()) is List)

    def test_get_sets_local_data(self):
        assert(type(self.mgr.sets.local.get_data()) is List)

if __name__ == '__main__':
    unittest.main()