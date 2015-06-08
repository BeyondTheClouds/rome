import unittest

from sqlalchemy.ext.declarative import declarative_base


BASE = declarative_base()

from lib.rome.driver.redis.lock import ClusterLock as ClusterLock


class TestLock(unittest.TestCase):
    def test_lock(self):

        dlm = ClusterLock()
        if dlm.lock("toto", 200):
            print("acquired lock")
            dlm.unlock("toto")
        else:
            print("could not acquire lock")
        pass


if __name__ == '__main__':
    unittest.main()