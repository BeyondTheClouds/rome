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



def fixed_ip_get_by_instance(instance_uuid):

    vif_and = and_(models.VirtualInterface.id ==
                   models.FixedIp.virtual_interface_id,
                   models.VirtualInterface.deleted == 0)
    result = Query(models.FixedIp).\
                 outerjoin(models.VirtualInterface, vif_and).\
                 all()

    # if not result:
    #     raise Exception()
    # TODO(Jonathan): quick fix
    return [x[0] for x in result]
    # return result

def _network_get_query():
    return Query(models.Network)

def network_get_all_by_host(host):
    fixed_host_filter = or_(models.FixedIp.host == host,
            and_(models.FixedIp.instance_uuid != None,
                 models.Instance.host == host))
    fixed_ip_query = Query(models.FixedIp.network_id).\
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


def network_get_associated_fixed_ips(network_id, host=None):
    # FIXME(sirp): since this returns fixed_ips, this would be better named
    # fixed_ip_get_all_by_network.
    # NOTE(vish): The ugly joins here are to solve a performance issue and
    #             should be removed once we can add and remove leases
    #             without regenerating the whole list
    vif_and = and_(models.VirtualInterface.id ==
                   models.FixedIp.virtual_interface_id,
                   models.VirtualInterface.deleted == 0)
    inst_and = and_(models.Instance.uuid == models.FixedIp.instance_uuid,
                    models.Instance.deleted == 0)

    query = Query(models.FixedIp.address,
                          models.FixedIp.instance_uuid,
                          models.FixedIp.network_id,
                          models.FixedIp.virtual_interface_id,
                          models.VirtualInterface.address,
                          models.Instance.hostname,
                          models.Instance.updated_at,
                          models.Instance.created_at,
                          models.FixedIp.allocated,
                          models.FixedIp.leased)
    query = query.join(models.VirtualInterface).join(models.Instance)
    query = query.filter(models.FixedIp.deleted == 0)
    query = query.filter(models.FixedIp.network_id == network_id)
    query = query.join((models.VirtualInterface, vif_and))
    query = query.filter(models.FixedIp.instance_uuid != None)
    query = query.filter(models.FixedIp.virtual_interface_id != None)

    # query = query.filter(models.FixedIp.deleted == 0).\
    #                filter(models.FixedIp.network_id == network_id).\
    #                join((models.VirtualInterface, vif_and)).\
    #                join((models.Instance, inst_and)).\
    #                filter(models.FixedIp.instance_uuid != None).\
    #                filter(models.FixedIp.virtual_interface_id != None)
    if host:
        query = query.filter(models.Instance.host == host)
    result = query.all()
    print(result)
    data = []
    for datum in result:
        cleaned = {}
        cleaned['address'] = datum[0]
        cleaned['instance_uuid'] = datum[1]
        cleaned['network_id'] = datum[2]
        cleaned['vif_id'] = datum[3]
        cleaned['vif_address'] = datum[4]
        cleaned['instance_hostname'] = datum[5]
        cleaned['instance_updated'] = datum[6]
        cleaned['instance_created'] = datum[7]
        cleaned['allocated'] = datum[8]
        cleaned['leased'] = datum[9]
        cleaned['default_route'] = datum[10] is not None
        data.append(cleaned)
    return data

def fixed_ip_disassociate_all_by_timeout(host, time):
    # NOTE(vish): only update fixed ips that "belong" to this
    #             host; i.e. the network host or the instance
    #             host matches. Two queries necessary because
    #             join with update doesn't work.
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
            filter(host_filter).all()
    print(result)
    return result

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

    # project_id = "448a3aeaab2c47259637a3a632748b8b"
    # group_names = ["default", "default2"]
    #
    # query = Query(models.SecurityGroup).\
    #         filter(models.SecurityGroup.name.in_(group_names))
    # result = query.all()
    # print(result)

    # fixed_ip_get_by_instance("1c5fc40a-abe1-48ee-829b-1be1c640fdf3")

    # network_get_all_by_host("econome-7")
    # network_get_associated_fixed_ips(1, None)
    # network_get_associated_fixed_ips(1, "econome-7")
    import datetime
    import pytz

    utc=pytz.UTC
    fixed_ip_disassociate_all_by_timeout("econome-7", utc.localize(datetime.datetime.now()))
    # query = Query(models.Network).filter(models.Network.id==1)
    # result = query.first()
    # print(result.share_address)


    pass