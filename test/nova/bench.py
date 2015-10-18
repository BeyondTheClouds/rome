__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging
import time
from lib.rome.core.lazy import LazyReference, LazyValue

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
    # create_mock_data(10, 20)

    # # fixed_ips = Query(models.FixedIp).filter(models.FixedIp.deleted==None).filter(models.FixedIp.deleted==None).filter(models.FixedIp.updated_at!=None).all()
    # # print(fixed_ips)
    #
    # network = Query(models.Network).filter_by(id=1).all()
    # print(network)



    # TEST1

    # def _aggregate_get_query(context, model_class, id_field=None, id=None,
    #                          session=None, read_deleted=None):
    #     columns_to_join = {models.Aggregate: ['_hosts', '_metadata']}
    #
    #     query = Query(model_class, session=session,
    #                         read_deleted=read_deleted)
    #
    #     # for c in columns_to_join.get(model_class, []):
    #     #     query = query.options(joinedload(c))
    #
    #     if id and id_field:
    #         query = query.filter(id_field == id)
    #
    #     return query
    #
    #
    # aggregate_id = "1"
    #
    # print("[aggregate_get] id:%s" % (aggregate_id))
    # query = _aggregate_get_query(None,
    #                              models.Aggregate,
    #                              models.Aggregate.id,
    #                              aggregate_id)
    # # aggregate = query.first()
    # from lib.rome.core.lazy import LazyReference
    # aggregate = LazyReference("aggregates", 1, None, None)
    # # aggregate.load_relationships()
    # print(aggregate)
    # print(aggregate.hosts)
    #
    # aggregate = Query(models.Aggregate).first()
    # print(aggregate)
    # print(aggregate.hosts)

    # TEST2

    # fixed_ip = models.FixedIp()
    # fixed_ip.network_id = 1
    # fixed_ip.address = "172.%d.%d.%d" % (255, 255, 3)
    # fixed_ip.save()
    # # fixed_ip.load_relationships()
    # # fixed_ip.network.load_relationships()
    # toto = fixed_ip.network.fixed_ips
    # # toto.__str__()
    # print(fixed_ip.network.fixed_ips)
    # print(fixed_ip.network.fixed_ips[0].network.fixed_ips)

    # query = Query(models.Network)
    # # network = query.first()
    # # print(network.created_at)
    # # # network.load_relationships()
    # # print(network.fixed_ips)
    #
    # for n in query.all():
    #     print(n.id)
    #     print(n.fixed_ips)
    #     print(len(n.fixed_ips))
    #     print(n.fixed_ips[0].network)

    # query = Query(models.Instance)
    # instances = query.all()
    # print(instances)

    info_caches = Query(models.InstanceInfoCache).all()
    instances = Query(models.Instance).all()

    i = 6
    info_caches[i].instance = instances[i]
    info_caches[i].save()
    print(info_caches[i].instance_uuid)
    print(info_caches[i].instance)
    print(instances[i].info_cache)

    i += 1
    info_caches[i].instance_uuid = instances[i].uuid
    info_caches[i].save()
    print(info_caches[i].instance_uuid)
    print(info_caches[i].instance)
    print(instances[i].info_cache)

    i += 1
    instances[i].info_cache = info_caches[i]
    instances[i].save()
    print(info_caches[i].instance_uuid)
    print(info_caches[i].instance)
    print(instances[i].info_cache)

    i += 1
    instances[i].info_cache = info_caches[i]
    info_caches[i].save()
    print(info_caches[i].instance_uuid)
    print(info_caches[i].instance)
    print(instances[i].info_cache)

    # info_cache = info_caches[0]
    # print(info_cache.instance_uuid)
    # # info_cache = info_caches[5]
    # print(info_cache.instance.uuid)
    # info_cache = info_caches[1]
    # print(info_cache.instance_uuid)
    # # info_cache = info_caches[5]
    # print(info_cache.instance.uuid)
    # # i = 0

    # for i in range(0, 2):
    #     instance = instances[i]
    #     print(instance.info_cache)
    #     # instance.info_cache = info_caches[i]
    #     info_cache = info_caches[i]
    #     instance.info_cache = info_cache
    #     instance.save()
    #     info_cache.save()
    #     # info_cache.save()
    #     # # instance.save()
    #     # print("> %s" % (info_cache.instance_uuid))
    #     # info_cache.load_relationships(debug=True)
    #     # print(">> %s" % (info_cache.instance))
    #     i += 1
    #     print(info_cache)