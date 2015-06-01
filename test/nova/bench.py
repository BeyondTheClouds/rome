__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging
import time

current_milli_time = lambda: int(round(time.time() * 1000))


def compute_ip(fixed_ip_id):
    pass


def bench_join(network_count=3, fixed_ip_count=200):

    for i in range(1, network_count):
        network = models.Network()
        network.id = i
        network.save()

    for i in range(1, network_count):
        for j in range(1, fixed_ip_count):
            fixed_ip = models.FixedIp()
            fixed_ip.id = i * fixed_ip_count + j
            fixed_ip.network_id = i
            fixed_ip.save()



if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)
    bench_join(3, 900)

    query = Query(models.FixedIp.id, models.Network.id).join(models.FixedIp.network_id == models.Network.id)
    result = query.all()

    # for each in result:
    #     print(each)
