__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession
import six
from oslo.utils import timeutils
from test.nova.methods.test_ensure_default_secgroup import _security_group_ensure_default, _security_group_get_query

import logging
import uuid
from lib.rome.core.orm.query import or_
from lib.rome.core.orm.query import and_
from sqlalchemy.sql import null
from nova.objects.fixed_ip import FixedIP


from nova.objects import base as obj_base
from nova import objects

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

from nova.openstack.common import uuidutils

def fixed_ip_get_by_instance(context, instance_uuid):
    if not uuidutils.is_uuid_like(instance_uuid):
        raise Exception("invalid UUID")

    vif_and = and_(models.VirtualInterface.id ==
                   models.FixedIp.virtual_interface_id,
                   models.VirtualInterface.deleted == 0)
    result = model_query(context, models.FixedIp, read_deleted="no").\
                 filter(models.FixedIp.instance_uuid==instance_uuid).\
                 outerjoin(models.VirtualInterface, vif_and).\
                 all()

    if not result:
        raise Exception("FixedIpNotFoundForInstance(instance_uuid=%s)" % (instance_uuid))
    # TODO(Jonathan): quick fix
    return [x[0] for x in result]

def get_by_instance_uuid(cls, context, instance_uuid):
    expected_attrs = ['network', 'virtual_interface', 'floating_ips']
    db_fixedips = fixed_ip_get_by_instance(context, instance_uuid)
    return obj_base.obj_make_list(context, cls(context),
                                  objects.FixedIP, db_fixedips,
                                  expected_attrs=expected_attrs)

class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("admin", "admin")
    instance_uuid = "ad86ce1e-c20f-40fa-a687-0f6dbbb0398f"
    print(get_by_instance_uuid(FixedIP, context, instance_uuid))
