__author__ = 'jonathan'

import logging
import time

import test.nova._fixtures as models
import uuid

current_milli_time = lambda: int(round(time.time() * 1000))


def compute_ip(network_id, fixed_ip_id):
    digits = [fixed_ip_id / 255, fixed_ip_id % 255]
    return "172.%d.%d.%d" % (network_id, digits[0], digits[1])


def create_mock_data(info_cache_count=200):

    for i in range(1, info_cache_count):
        info_cache = models.InstanceInfoCache()
        info_cache.id = i
        info_cache.instance_uuid = str(uuid.uuid1())
        info_cache.save()


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)
    create_mock_data(2000)
