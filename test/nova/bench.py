__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging
import time

current_milli_time = lambda: int(round(time.time() * 1000))

if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    # for i in xrange(0, 10000):
    #     network = models.Network()
    #     network.uuid = str(i)
    #     network.save()

    time1 = current_milli_time()
    # result = Query(models.InstanceSystemMetadata).all()
    # print(result[0]["network_id"])

    rows = Query(models.FixedIp, models.Network).filter(models.FixedIp.network_id, models.Network.id).all()
    print(rows)

    for row in rows:
        print(row)

    # result = Query(models.Network).first()
    # print(len(result))
    print(rows[0])
    # print(result)
    # print(result.share_address)

    # print(result)
    # print(time2 - time1)