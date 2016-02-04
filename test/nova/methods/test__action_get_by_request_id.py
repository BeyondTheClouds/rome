__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession
import six
# from oslo.utils import timeutils
from lib.rome.core.utils import timeutils
from test.nova.methods.test_ensure_default_secgroup import _security_group_ensure_default, _security_group_get_query

import logging
import uuid
from lib.rome.core.orm.query import or_
from lib.rome.core.orm.query import and_
from sqlalchemy.sql import null
import collections

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



def _action_get_by_request_id(context, instance_uuid, request_id,
                                                                session=None):
    result = model_query(context, models.InstanceAction, session=session).\
                         filter_by(instance_uuid=instance_uuid).\
                         filter_by(request_id=request_id).\
                         first()
    return result

class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("admin", "admin")

    # host = "jonathan-VirtualBox"
    host = "edel-17"

    # import yappi
    # yappi.start()

    instance_uuid = "8287a138-4eb9-4164-aed4-bc474b7e119f"
    request_uuid = "req-0cb566ca-a75e-485d-af3c-0a1a9d5179b5"
    print(_action_get_by_request_id(context, instance_uuid, request_uuid))

    # yappi.get_func_stats().print_all()
    # fixed_ips = Query(models.FixedIp).filter(models.FixedIp.updated_at!=None).all()
    # print(fixed_ips[0].updated_at)
