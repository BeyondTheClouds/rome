__author__ = 'jonathan'

import logging
import time

import test.nova._fixtures as models

current_milli_time = lambda: int(round(time.time() * 1000))


def compute_ip(network_id, fixed_ip_id):
    digits = [fixed_ip_id / 255, fixed_ip_id % 255]
    return "172.%d.%d.%d" % (network_id, digits[0], digits[1])


def create_mock_data(network_count=3, fixed_ip_count=200):

    for i in range(1, network_count):
        network = models.Network()
        network.id = i
        network.fixed_ips = []
        # network.cidr = IP
        network.save()

    for i in range(1, network_count):
        for j in range(1, fixed_ip_count):
            fixed_ip = models.FixedIp()
            fixed_ip.id = i * fixed_ip_count + j
            fixed_ip.network_id = i
            fixed_ip.address = compute_ip(i, j)
            fixed_ip.save()
    pass


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)
    create_mock_data(40, 40)
