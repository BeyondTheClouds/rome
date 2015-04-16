__author__ = 'jonathan'

import unittest
import threading
import lib.rome.utils.MemoizationDecorator as MemoizationDecorator


class TestMemoization(unittest.TestCase):
    def test_memoization(self):
        import time

        class Foo(object):
            def get_magical_value(self, cpt):
                print("starting")
                time.sleep(7)
                print("ending")
                return cpt

        # obj1 = Foo()
        obj1 = MemoizationDecorator(Foo())

        def do_request():
            value = obj1.get_magical_value(42)
            print(value)

        for n in range(2):
            thread = threading.Thread(target=do_request)
            thread.start()
            time.sleep(1)
        time.sleep(3)
        for n in range(3):
            thread = threading.Thread(target=do_request)
            thread.start()
            time.sleep(1)


if __name__ == '__main__':
    unittest.main()