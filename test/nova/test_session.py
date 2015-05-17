__author__ = 'jonathan'

import unittest
from lib.rome.core.session.session import Session as Session
from lib.rome.core.orm.query import Query as Query
from test.test_dogs import *

class TestMemoization(unittest.TestCase):

    def test_session_execution(self):
        session = Session()
        with session.begin():
            dogs = Query(Dog).all()
            print("dogs: %s" % (dogs))
            print("session_id: %s" % (session.session_id))
            print dogs[0].rid
            print dogs[0].save()
            raise Exception("toto")


if __name__ == '__main__':
    unittest.main()