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


def _network_get_query(context, session=None):
    return model_query(context, models.Network, session=session,
                       read_deleted="no")

def network_get_all_by_host(context, host):
    session = get_session()

    network_ids1 = Query(models.Network, models.Network.id).filter(models.Network.host==host).all()
    network_ids2 = Query(models.Network, models.Network.id).join(models.FixedIp)\
        .filter(models.Network.id==models.FixedIp.network_id)\
        .filter(models.FixedIp.host==host)\
        .all()
    network_ids3 = Query(models.Network, models.Network.id).join(models.FixedIp).join(models.Instance)\
        .filter(models.Network.id==models.FixedIp.network_id)\
        .filter(models.Instance.uuid==models.FixedIp.instance_uuid)\
        .filter(models.Instance.host==host)\
        .all()
    # network_ids = Query(models.Network, models.Network.id).filter(models.Network.host==host).all()

    # fixed_host_filter = or_(models.FixedIp.host == host,
    #         and_(models.FixedIp.instance_uuid != None,
    #              models.Instance.host == host))
    # fixed_ip_query = model_query(context, models.FixedIp.network_id,
    #                              base_model=models.FixedIp,
    #                              session=session).\
    #                  outerjoin((models.Instance,
    #                             models.Instance.uuid ==
    #                             models.FixedIp.instance_uuid)).\
    #                  filter(fixed_host_filter)
    # # NOTE(vish): return networks that have host set
    # #             or that have a fixed ip with host set
    # #             or that have an instance with host set
    # host_filter = or_(models.Network.host == host,
    #                   models.Network.id.in_(fixed_ip_query.subquery()))
    # return _network_get_query(context, session=session).\
    #                    filter(host_filter).\
    #                    all()
    processed_pairs = []
    result = []
    for pair in network_ids1 + network_ids2 + network_ids3:
        if pair[1] not in processed_pairs:
            processed_pairs += [pair[1]]
            result += [pair[0]]
    return result




class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("admin", "admin")

    print(network_get_all_by_host(context, "jonathan-VirtualBox"))

    fixed_ips = Query(models.FixedIp).filter(models.FixedIp.updated_at!=None).all()
    print(fixed_ips[0].updated_at)
