__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query
from lib.rome.core.orm.query import or_

current_milli_time = lambda: int(round(time.time() * 1000))

from threading import Thread
import time

class SelectorThread(Thread):
    def __init__(self, lettre):
        Thread.__init__(self)
        self.lettre = lettre

    def run(self):
        # print("ping")
        # query = Query(models.FixedIp)
        query = Query(models.FixedIp).join(models.Network, models.Network.id==models.FixedIp.network_id)

        # query = Query(models.Instance)
        # query = Query(models.FixedIp).join(models.Network, models.Network.id==models.FixedIp.network_id).filter(or_(models.FixedIp.address=="172.9.0.15", models.FixedIp.address=="172.9.0.16", models.FixedIp.address=="172.9.0.17", models.FixedIp.address=="172.9.0.18", models.FixedIp.address=="172.9.0.19"))
        print(len(query.all()))
        # print("pong")

if __name__ == '__main__':

    import logging
    logging.getLogger().setLevel(logging.DEBUG)

    n = 1

    for i in range(0, n):
        thread_1 = SelectorThread("canard")
        # thread_2 = SelectorThread("TORTUE")

        thread_1.start()
        # thread_2.start()

        thread_1.join()
        # thread_2.join()


