__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging

def _instance_pcidevs_get_multi(context, instance_uuids, session=None):
    return Query(models.PciDevice, session=session).\
        filter_by(status='allocated').\
        filter(models.PciDevice.instance_uuid.in_(instance_uuids))

# def _instance_system_metadata_get_multi(context, instance_uuids,
#                                         session=None, use_slave=False):
#
#     # for instance_uuid in instance_uuids:
#     #     query = model_query(context, models.InstanceSystemMetadata,
#     #         session=session, use_slave=use_slave).\
#     #         filter(models.InstanceSystemMetadata.instance_uuid==instance_uuid)
#     #     result += query.all()
#
#     return result

def _instance_metadata_get_multi(context, instance_uuids,
                                 session=None, use_slave=False):
    if not instance_uuids:
        return []
    metadata_list = Query(models.InstanceMetadata).all()
    result = filter(lambda m: m.instance_uuid in instance_uuids, metadata_list)
    return result

def _instances_fill_metadata(context, instances,
                             manual_joins=None, use_slave=False):
    """Selectively fill instances with manually-joined metadata. Note that
    instance will be converted to a dict.

    :param context: security context
    :param instances: list of instances to fill
    :param manual_joins: list of tables to manually join (can be any
                         combination of 'metadata' and 'system_metadata' or
                         None to take the default of both)
    """

    def flatten(l):
        return [item for sublist in l for item in sublist]

    uuids = [inst['uuid'] for inst in instances]

    if manual_joins is None:
        manual_joins = ['metadata', 'system_metadata']

    meta = collections.defaultdict(list)
    if 'system_metadata' in manual_joins:
        for instance in instances:
            for metadata in instance.metadata:
                meta[instance.uuid].append(metadata)

    sys_meta = collections.defaultdict(list)
    if 'system_metadata' in manual_joins:
        for instance in instances:
            for system_metadata in instance.system_metadata:
                sys_meta[instance.uuid].append(system_metadata)

    pcidevs = collections.defaultdict(list)
    if 'pci_devices' in manual_joins:
        for row in _instance_pcidevs_get_multi(context, uuids):
            pcidevs[row['instance_uuid']].append(row)

    filled_instances = []
    for inst in instances:
        inst = dict(inst.iteritems())
        # inst['system_metadata'] = sys_meta[inst['uuid']]
        inst['metadata'] = meta[inst['uuid']]
        if 'pci_devices' in manual_joins:
            inst['pci_devices'] = pcidevs[inst['uuid']]
        filled_instances.append(inst)

    return filled_instances

from lib.rome.core.orm.query import or_
from lib.rome.core.orm.query import and_

def _network_get_query():
    return Query(models.Network)

def network_get_all_by_host(context, host):
    # session = get_session()
    fixed_host_filter = or_(models.FixedIp.host == host,
            and_(models.FixedIp.instance_uuid != None,
                 models.Instance.host == host))
    fixed_ip_query = Query(models.FixedIp.network_id,
                                 base_model=models.FixedIp).\
                     outerjoin((models.Instance,
                                models.Instance.uuid ==
                                models.FixedIp.instance_uuid)).\
                     filter(fixed_host_filter)
    # NOTE(vish): return networks that have host set
    #             or that have a fixed ip with host set
    #             or that have an instance with host set
    host_filter = or_(models.Network.host == host,
                      models.Network.id.in_(fixed_ip_query.subquery()))
    return _network_get_query().\
                       filter(host_filter).\
                       all()

from oslo.utils import timeutils

def fixed_ip_disassociate_all_by_timeout(host, time):
    #             host matches. Two queries necessary because
    import time
    host_filter = or_(and_(models.Instance.host == host,
                           models.Network.multi_host == True),
                      models.Network.host == host)
    result = Query(models.FixedIp.id).\
            filter(models.FixedIp.leased == True).\
            filter(models.FixedIp.allocated == False).\
            filter(models.FixedIp.updated_at < time).\
            join((models.Network,
                  models.Network.id == models.FixedIp.network_id)).\
            join((models.Instance,
                  models.Instance.uuid == models.FixedIp.instance_uuid)).\
            filter(host_filter).\
            update({'instance_uuid': None,
                                 'leased': False,
                                 'updated_at': timeutils.utcnow()},
                                synchronize_session='fetch')

if __name__ == '__main__':


    logging.getLogger().setLevel(logging.DEBUG)

    # host = "econome-18"
    # # query = Query(models.Instance).filter(models.Instance.host==host)
    # query = Query(models.Instance).filter_by(host=host)
    # result = query.all()
    # _instances_fill_metadata(None, result)
    # print(map(lambda x: x.id, result))
    # # query = Query(models.Network).filter_by(id=1)
    # # result = query.all()
    # # print(result)

    # result = network_get_all_by_host(None, "granduc-4.luxembourg.grid5000.fr")
    # fixed_ip_disassociate_all_by_timeout("granduc-9.luxembourg.grid5000.fr", timeutils.utcnow())

    project_id = "448a3aeaab2c47259637a3a632748b8b"
    group_names = ["default", "default2"]

    query = Query(models.SecurityGroup).\
            filter(models.SecurityGroup.name.in_(group_names))
    result = query.all()
    print(result)


    # query = Query(models.Network).filter(models.Network.id==1)
    # result = query.first()
    # print(result.share_address)


    pass