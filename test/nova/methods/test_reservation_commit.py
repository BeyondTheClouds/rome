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

def _quota_reservations_query(session, context, reservations):
    """Return the relevant reservations."""

    # Get the listed reservations
    return model_query(context, models.Reservation,
                       read_deleted="no",
                       session=session).\
                   filter(models.Reservation.uuid.in_(reservations)).\
                   with_lockmode('update')

def _get_project_user_quota_usages(context, session, project_id,
                                   user_id):
    rows = model_query(context, models.QuotaUsage,
                       read_deleted="no",
                       session=session).\
                   filter_by(project_id=project_id).\
                   with_lockmode('update').\
                   all()
    proj_result = dict()
    user_result = dict()
    # Get the total count of in_use,reserved
    for row in rows:
        proj_result.setdefault(row.resource,
                               dict(in_use=0, reserved=0, total=0))
        proj_result[row.resource]['in_use'] += row.in_use
        proj_result[row.resource]['reserved'] += row.reserved
        proj_result[row.resource]['total'] += (row.in_use + row.reserved)
        if row.user_id is None or row.user_id == user_id:
            user_result[row.resource] = row
    return proj_result, user_result

def reservation_commit(context, reservations, project_id=None, user_id=None):
    session = get_session()
    with session.begin():
        _project_usages, user_usages = _get_project_user_quota_usages(
                context, session, project_id, user_id)
        reservation_query = _quota_reservations_query(session, context,
                                                      reservations)
        for reservation in reservation_query.all():
            usage = user_usages[reservation.resource]
            if reservation.delta >= 0:
                usage.reserved -= reservation.delta
            usage.in_use += reservation.delta
            session.add(usage)
        reservation_query.soft_delete(synchronize_session=False)



class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    reservations = [u'b6f9cabb-8c1a-4a62-8de9-c5bbcd78b9f4', u'bfbee48a-ffb0-4417-a6ed-7380ac184662', u'94261b8e-d88e-4777-955a-c8a5eba0d141']
    project_id = u'ac4f114485eb43caa371b84b362988f6'
    user_id = u'a76f0bab847d448786fcf6d65c90f985'

    context = Context("admin", "admin")
    reservation_commit(context, reservations, project_id, user_id)
