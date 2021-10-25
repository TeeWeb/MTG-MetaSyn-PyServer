from types import resolve_bases
import unittest
from app import create_app

class TestApp(unittest.TestCase):

    def test_get_types_data(self):
        test_app = create_app
        test_app.retri
        responseCode = 11
        self.assertEquals(responseCode, 200)

if __name__ == '__main__':
    unittest.main()