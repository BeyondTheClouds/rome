import unittest

from sqlalchemy.ext.declarative import declarative_base


BASE = declarative_base()

from lib.rome.driver.redis.lock import ClusterLock as ClusterLock


class TestLock(unittest.TestCase):

    # def test_lock_simple(self):
    #     dlm = ClusterLock()
    #     if dlm.lock("toto", 2000):
    #         print("acquired lock")
    #         dlm.unlock("toto")
    #     else:
    #         print("could not acquire lock")

    def test_lock_concurrent(self):
        import threading
        import time

        def work1():
            dlm = ClusterLock()
            while not dlm.lock("toto", 2000):
                print("1> waiting for acquiring lock")
                time.sleep(0.1)
            print("1> acquired lock")
            time.sleep(3)
            dlm.unlock("toto")

        def work2():
            time.sleep(0.1)
            dlm = ClusterLock()
            while not dlm.lock("toto", 2000):
                print("2> waiting for acquiring lock")
                time.sleep(0.1)
            print("2> acquired lock")
            time.sleep(3)
            dlm.unlock("toto")

        a = threading.Thread(None, work1(), None)
        b = threading.Thread(None, work2(), None)
        a.start()
        b.start()

        # a.join()


if __name__ == '__main__':
    unittest.main()