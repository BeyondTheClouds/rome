__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query
from lib.rome.core.orm.query import or_

current_milli_time = lambda: int(round(time.time() * 1000))

from threading import Thread
import time

class SelectorThread(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        query = Query(models.FixedIp).join(models.Network, models.Network.id==models.FixedIp.network_id)
        query.all()

if __name__ == '__main__':

    import logging
    logging.getLogger().setLevel(logging.DEBUG)

    n = 100

    for i in range(0, n):
        thread_1 = SelectorThread()
        thread_1.start()
        thread_1.join()
