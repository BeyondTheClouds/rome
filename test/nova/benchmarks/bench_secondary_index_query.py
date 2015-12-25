__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query
from lib.rome.core.orm.query import or_

current_milli_time = lambda: int(round(time.time() * 1000))

from threading import Thread
import time

instance_uuid = ""

class SelectorThread(Thread):
    def __init__(self, instance_uuid):
        Thread.__init__(self)
        self.instance_uuid = instance_uuid

    def run(self):
        result = Query(models.InstanceInfoCache).\
                         filter_by(instance_uuid=instance_uuid).\
                         first()
        print(result)

if __name__ == '__main__':

    import logging
    logging.getLogger().setLevel(logging.DEBUG)

    n = 1

    one_info_cache = Query(models.InstanceInfoCache).first()

    for i in range(0, n):
        thread_1 = SelectorThread(one_info_cache.instance_uuid)
        thread_1.start()
        thread_1.join()


