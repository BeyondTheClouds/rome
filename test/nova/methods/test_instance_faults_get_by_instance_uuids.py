__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

from test.nova.methods.test_ensure_default_secgroup import _security_group_ensure_default, _security_group_get_query

import logging
import uuid
import itertools

from oslo.serialization import jsonutils

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


def instance_fault_get_by_instance_uuids(context, instance_uuids):
    """Get all instance faults for the provided instance_uuids."""
    if not instance_uuids:
        return {}

    rows = model_query(context, models.InstanceFault, read_deleted='no').\
                       filter(models.InstanceFault.instance_uuid.in_(
                           instance_uuids)).\
                       all()

    output = {}
    for instance_uuid in instance_uuids:
        output[instance_uuid] = []

    for row in rows:
        data = dict(row.iteritems())
        output[row['instance_uuid']].append(data)

    return output

def obj_make_list(context, list_obj, item_cls, db_list, **extra_args):
    """Construct an object list from a list of primitives.
    This calls item_cls._from_db_object() on each item of db_list, and
    adds the resulting object to list_obj.
    :param:context: Request contextr
    :param:list_obj: An ObjectListBase object
    :param:item_cls: The NovaObject class of the objects within the list
    :param:db_list: The list of primitives to convert to objects
    :param:extra_args: Extra arguments to pass to _from_db_object()
    :returns: list_obj
    """
    list_obj.objects = []
    for db_item in db_list:
        item = _from_db_object(context, item_cls(), db_item,
                                        **extra_args)
        list_obj.objects.append(item)
    list_obj._context = context
    list_obj.obj_reset_changes()
    return list_obj

def _from_db_object(context, fault, db_fault):
        # NOTE(danms): These are identical right now
        for key in fault.fields:
            fault[key] = db_fault[key]
        fault._context = context
        fault.obj_reset_changes()
        return fault

def get_by_instance_uuids(cls, context, instance_uuids):
        db_faultdict = instance_fault_get_by_instance_uuids(context,
                                                               instance_uuids)
        db_faultlist = itertools.chain(*db_faultdict.values())
        return obj_make_list(context, cls(context), InstanceFault,
                                  db_faultlist)

class InstanceFault(object):
    # Version 1.0: Initial version
    # Version 1.1: String attributes updated to support unicode
    # Version 1.2: Added create()
    VERSION = '1.2'

    fields = {
        'id': -1,
        'instance_uuid': "",
        'code': -1,
        'message': "",
        'details': "",
        'host': "",
        }

class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id

def test_instance_faults_get_by_instance_uuids():
    instances = Query(models.Instance).all()
    instances_uuids = map(lambda x: x.uuid, instances)

    get_by_instance_uuids(InstanceFault, context, instances_uuids)

if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("project1", "user1")

    test_instance_faults_get_by_instance_uuids()
