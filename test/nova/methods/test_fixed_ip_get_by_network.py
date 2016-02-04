__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

from nova import objects
from nova.objects import base as obj_base

from test.nova.methods.test_ensure_default_secgroup import _security_group_ensure_default, _security_group_get_query
from lib.rome.core.orm.query import or_
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import joinedload_all

import logging
import uuid
# from oslo.utils import timeutils
from lib.rome.core.utils import timeutils
from sqlalchemy.sql.expression import asc
from sqlalchemy.sql.expression import desc
from lib.rome.core.orm.query import or_
from lib.rome.core.orm.query import and_
from nova.objects.network import Network

LOG = logging.getLogger()

# List of fields that can be joined in DB layer.
_INSTANCE_OPTIONAL_JOINED_FIELDS = ['metadata', 'system_metadata',
                                    'info_cache', 'security_groups',
                                    'pci_devices']
# These are fields that are optional but don't translate to db columns
_INSTANCE_OPTIONAL_NON_COLUMN_FIELDS = ['fault', 'numa_topology',
                                        'pci_requests']

# These are fields that can be specified as expected_attrs
INSTANCE_OPTIONAL_ATTRS = (_INSTANCE_OPTIONAL_JOINED_FIELDS +
                           _INSTANCE_OPTIONAL_NON_COLUMN_FIELDS)

def get_session(use_slave=False, **kwargs):
    # return FakeSession()
    return RomeSession()
    # return OldRomeSession()


def model_query(context, *args, **kwargs):
    # base_model = kwargs["base_model"]
    # models = args
    return RomeQuery(*args, **kwargs)


def network_get_associated_fixed_ips(context, network_id, host=None):
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
    session = get_session()
    query = session.query(models.FixedIp.address,
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
    data = []
    for datum in result:
        if datum[3] is None:
            print("ici?")
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

def get_by_network(cls, context, network, host=None):
        ipinfo = network_get_associated_fixed_ips(context,
                                                     network['id'],
                                                     host=host)
        if not ipinfo:
            return []

        fips = cls(context=context, objects=[])

        for info in ipinfo:
            value = info['vif_address']
            if value is None:
                raise Exception("%s has a None address field" % (info))
            continue
            inst = objects.Instance(context=context,
                                    uuid=info['instance_uuid'],
                                    hostname=info['instance_hostname'],
                                    created_at=info['instance_created'],
                                    updated_at=info['instance_updated'])
            vif = objects.VirtualInterface(context=context,
                                           id=info['vif_id'],
                                           address=info['vif_address'])
            fip = objects.FixedIP(context=context,
                                  address=info['address'],
                                  instance_uuid=info['instance_uuid'],
                                  network_id=info['network_id'],
                                  virtual_interface_id=info['vif_id'],
                                  allocated=info['allocated'],
                                  leased=info['leased'],
                                  default_route=info['default_route'],
                                  instance=inst,
                                  virtual_interface=vif)
            fips.objects.append(fip)
        fips.obj_reset_changes()
        return fips


class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id

class ModelInstance(dict):
    def __init__(self):
        self.fields = []
        self.deleted = None
        self.cleaned = None

if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("project1", "user1")

    host = "jonathan-VirtualBox"

    get_by_network(Network, context, {"id": 1})
