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

def service_get_by_compute_host(context, host):
    result = model_query(context, models.Service, read_deleted="no").filter_by(host=host).\
                first()

    if not result:
        raise Exception(host=host)

    return result




class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("admin", "admin")

    host = "jonathan-VirtualBox"
    # host = "edel-17"

    service = service_get_by_compute_host(context, host)
    import json

    service = {'binary': 'nova-conductor', 'deleted': 0.0, 'created_at': {'timezone': 'None', 'simplify_strategy': 'datetime', 'value': 'Dec 09 2015 17:17:58'}, 'updated_at': {'timezone': 'None', 'simplify_strategy': 'datetime', 'value': 'Dec 09 2015 17:34:03'}, 'report_count': 17, 'topic': 'conductor', 'host': 'jonathan-VirtualBox', 'disabled': False, 'deleted_at': None, 'disabled_reason': None, 'id': 1}

    json.dumps(service)

