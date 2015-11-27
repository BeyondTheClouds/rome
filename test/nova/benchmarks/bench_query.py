__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging
import time
from lib.rome.core.lazy import LazyReference, LazyValue
from lib.rome.core.orm.query import or_

current_milli_time = lambda: int(round(time.time() * 1000))

import random
import sys
from threading import Thread
import time

class SelectorThread(Thread):
    def __init__(self, lettre):
        Thread.__init__(self)
        self.lettre = lettre

    def run(self):
        print("ping")
        query = Query(models.FixedIp).filter(or_(models.FixedIp.address=="172.9.0.15", models.FixedIp.address=="172.9.0.16", models.FixedIp.address=="172.9.0.17", models.FixedIp.address=="172.9.0.18", models.FixedIp.address=="172.9.0.19"))
        print(len(query.all()))
        print("pong")

if __name__ == '__main__':


    import yappi
    yappi.start()

    # logging.getLogger().setLevel(logging.DEBUG)

    n = 10

    for i in range(0, n):
        thread_1 = SelectorThread("canard")
        thread_2 = SelectorThread("TORTUE")

        thread_1.start()
        thread_2.start()

        thread_1.join()
        thread_2.join()

    yappi.get_func_stats().print_all()

