__author__ = 'jonathan'

import logging

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query
from lib.rome.core.session.session import Session as Session


def test_relationships_single_str(save_instance=True, save_info_cache=True, use_update=False, use_session=False):
    print("Ensure that foreign keys are working test_relationships_single_str(save_instance=%s, save_info_cache=%s, use_update=%s, use_session=%s)" % (save_instance, save_info_cache, use_update, use_session))

    session = None
    if use_session:
        session = Session()

    instance_count = Query(models.Instance).count()

    instance = models.Instance()
    instance.uuid = "uuid_%s" % (instance_count)
    if save_instance:
        if use_session:
            session.add(instance)
        else:
            instance.save()

    instance_info_cache = models.InstanceInfoCache()

    if not use_update:
        instance_info_cache.instance_uuid = instance.uuid
    else:
        instance_info_cache.update({"instance_uuid": instance.uuid})

    if not save_info_cache:
        if use_session:
            session.add(instance)
        else:
            instance.save()
    else:
        if use_session:
            session.add(instance_info_cache)
        else:
            instance_info_cache.save()

    if use_session:
        session.flush()

    instance_from_db = Query(models.Instance, models.Instance.id==instance.id).first()
    instance_info_cache_from_db = Query(models.InstanceInfoCache, models.InstanceInfoCache.id==instance_info_cache.id).first()

    assert instance_from_db.id == instance.id
    assert instance_info_cache_from_db.id == instance_info_cache.id

    assert instance_from_db.info_cache is not None
    assert instance_from_db.info_cache.id == instance_info_cache.id

    assert instance_info_cache_from_db.instance is not None
    assert instance_info_cache_from_db.instance.id == instance.id
    assert instance_info_cache_from_db.instance_uuid == instance.uuid


def test_relationships_single_object(save_instance=True, save_info_cache=True, use_update=False, update_instance=False, use_session=False):
    print("Ensure that foreign keys are working test_relationships_single_object(save_instance=%s, save_info_cache=%s, use_update=%s, update_instance=%s, use_session=%s)" % (save_instance, save_info_cache, use_update, update_instance, use_session))

    session = None
    if use_session:
        session = Session()

    instance_count = Query(models.Instance).count()

    instance = models.Instance()
    instance_uuid = "uuid_%s" % (instance_count)

    if save_instance:
        if use_session:
            session.add(instance)
        else:
            instance.save()

    instance_info_cache = models.InstanceInfoCache()

    if update_instance:
        if not use_update:
            instance.info_cache = instance_info_cache
            instance.uuid = instance_uuid
        else:
            # CLASSIC
            # instance.update({"info_cache": instance_info_cache})
            # DEBUG
            values = {}
            values['uuid'] = instance_uuid
            # instance['info_cache'] = models.InstanceInfoCache()
            instance['info_cache'] = instance_info_cache
            info_cache = values.pop('info_cache', None)
            if info_cache is not None:
                instance['info_cache'].update(info_cache)
            instance.update(values, do_save=False)
        if not save_info_cache:
            if use_session:
                session.add(instance)
            else:
                instance.save()
        else:
            if use_session:
                session.add(instance_info_cache)
            else:
                instance_info_cache.save()
    else:
        instance.uuid = instance_uuid
        if not use_update:
            instance_info_cache.instance = instance
        else:
            instance_info_cache.update({"instance": instance})
        if not save_info_cache:
            instance.save()
        else:
            if use_session:
                session.add(instance_info_cache)
            else:
                instance_info_cache.save()

    if use_session:
        session.flush()

    instance_from_db = Query(models.Instance, models.Instance.id==instance.id).first()
    instance_info_cache_from_db = Query(models.InstanceInfoCache, models.InstanceInfoCache.id==instance_info_cache.id).first()

    assert instance_from_db.id == instance.id
    assert instance_info_cache_from_db.id == instance_info_cache.id

    assert instance_from_db.info_cache is not None
    assert instance_from_db.info_cache.id == instance_info_cache.id

    assert instance_info_cache_from_db.instance is not None
    assert instance_info_cache_from_db.instance.id == instance.id
    assert instance_info_cache_from_db.instance_uuid == instance.uuid


def test_relationships_list_int(save_fixed_ip=True):
    print("Ensure that foreign keys are working test_relationships_list_int(save_fixed_ip=%s)" % (save_fixed_ip))

    network = models.Network()
    network.save()

    fixed_ips = []
    for i in range(0, 5):
        fixed_ip = models.FixedIp()
        fixed_ip.network_id = network.id
        fixed_ips += [fixed_ip]
        if not save_fixed_ip:
            fixed_ip.network = network
            network.save()
        else:
            fixed_ip.save()

    network_from_db = Query(models.Network, models.Network.id==network.id).first()

    for fixed_ip in fixed_ips:

        fixed_ip_from_db = Query(models.FixedIp, models.FixedIp.network_id==network.id, models.FixedIp.id==fixed_ip.id).first()

        assert network_from_db.id == network.id
        assert fixed_ip_from_db.id == fixed_ip.id

        network_from_db.load_relationships()

        assert network_from_db.fixed_ips is not None and len(network_from_db.fixed_ips) > 0
        assert fixed_ip_from_db.id in map(lambda x: x.id, network_from_db.fixed_ips)

        assert fixed_ip_from_db.network is not None
        assert fixed_ip_from_db.network.id == network_from_db.id
        assert fixed_ip_from_db.network_id == network_from_db.id


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    test_relationships_single_object(save_instance=True, save_info_cache=True, use_update=True, update_instance=True, use_session=True)
    test_relationships_single_object(save_instance=True, save_info_cache=True, use_update=True, update_instance=True, use_session=True)

    # sys.exit(0)

    ######################
    # Instance/InfoCache #
    ######################

    test_relationships_single_str(save_instance=True, save_info_cache=True, use_update=False, use_session=True)
    test_relationships_single_object(save_instance=True, save_info_cache=True, use_update=True, update_instance=True, use_session=True)

    for use_session in [True, False]:
        test_relationships_single_str(use_session=use_session)
        test_relationships_single_str(use_update=True, use_session=use_session)

    for use_session in [True, False]:
        for use_update in [True, False]:
            for update_instance in [True, False]:
                test_relationships_single_object(use_update=use_update, update_instance=update_instance, use_session=use_session)
                test_relationships_single_object(save_instance=False, use_update=use_update, update_instance=update_instance, use_session=use_session)

                test_relationships_single_object(save_info_cache=False, use_update=use_update, update_instance=update_instance, use_session=use_session)
                test_relationships_single_object(save_instance=False, save_info_cache=False, use_update=use_update, update_instance=update_instance, use_session=use_session)

    ######################
    # Network/FixedIp    #
    ######################

    test_relationships_list_int()
    test_relationships_list_int(save_fixed_ip=False)
