__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging
import time

current_milli_time = lambda: int(round(time.time() * 1000))

if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    # # for i in xrange(0, 10000):
    # #     network = models.Network()
    # #     network.uuid = str(i)
    # #     network.save()
    #
    # time1 = current_milli_time()
    # # result = Query(models.InstanceSystemMetadata).all()
    # # print(result[0]["network_id"])
    #
    # # rows = Query(models.Network, models.FixedIp).filter(models.Network.id==models.FixedIp.network_id).all()
    # rows = Query(models.Network).filter(models.Network.id==1).all()
    # print(rows)
    #
    # for row in rows:
    #     print(row)
    #
    # # result = Query(models.Network).first()
    # # print(len(result))
    # print(rows[0])
    # # print(result)
    # # print(result.share_address)
    #
    # # print(result)
    # # print(time2 - time1)

    result = Query(models.Network).join(models.FixedIp, models.FixedIp.network_id==models.Network.id).all()
    print(result)

    host = "granduc-15"
    binary = "nova-conductor"

    result = Query(models.Service).\
                     filter_by(host=host).\
                     filter_by(binary=binary).\
                     first()
    print(result)