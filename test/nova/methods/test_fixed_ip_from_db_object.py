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

from nova.objects.instance import Instance
from nova.objects.network import Network
from nova.objects.virtual_interface import VirtualInterface
from nova.objects.fixed_ip import FixedIP

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

FIXED_IP_OPTIONAL_ATTRS = ['instance', 'network', 'virtual_interface',
                           'floating_ips']

from nova.objects import base as obj_base
from nova import objects

from nova.objects.fixed_ip import FixedIP

def _from_db_object(context, fixedip, db_fixedip, expected_attrs=None):
        if expected_attrs is None:
            expected_attrs = []
        for field in fixedip.fields:
            if field == 'default_route':
                # NOTE(danms): This field is only set when doing a
                # FixedIPList.get_by_network() because it's a relatively
                # special-case thing, so skip it here
                continue
            if field not in FIXED_IP_OPTIONAL_ATTRS:
                fixedip[field] = db_fixedip[field]
        # NOTE(danms): Instance could be deleted, and thus None
        if 'instance' in expected_attrs:
            fixedip.instance = objects.Instance._from_db_object(
                context,
                objects.Instance(context),
                db_fixedip['instance']) if db_fixedip['instance'] else None
        if 'network' in expected_attrs:
            fixedip.network = objects.Network._from_db_object(
                context,
                objects.Network(context),
                db_fixedip['network']) if db_fixedip['network'] else None
        if 'virtual_interface' in expected_attrs:
            db_vif = db_fixedip['virtual_interface']
            vif = objects.VirtualInterface._from_db_object(
                context,
                objects.VirtualInterface(context),
                db_fixedip['virtual_interface']) if db_vif else None
            fixedip.virtual_interface = vif
        if 'floating_ips' in expected_attrs:
            fixedip.floating_ips = obj_base.obj_make_list(
                    context, objects.FloatingIPList(context),
                    objects.FloatingIP, db_fixedip['floating_ips'])
        fixedip._context = context
        fixedip.obj_reset_changes()
        return fixedip


class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("admin", "admin")

    fixed_ips = Query(models.FixedIp).all()
    for fixed_ip in fixed_ips:
        result = _from_db_object(context, FixedIP(), fixed_ip)
        print(result)

