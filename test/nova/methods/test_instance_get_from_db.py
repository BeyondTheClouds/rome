__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

import logging

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
        item = item_cls._from_db_object(context, item_cls(), db_item,
                                        **extra_args)
        list_obj.objects.append(item)
    list_obj._context = context
    list_obj.obj_reset_changes()
    return list_obj

def instance_fault_get_by_instance_uuids(context, instance_uuids):
    """Get all instance faults for the provided instance_uuids."""
    if not instance_uuids:
        return {}

    rows = model_query(context, models.InstanceFault, read_deleted='no').\
                       filter(models.InstanceFault.instance_uuid.in_(
                           instance_uuids)).all()

    output = {}
    for instance_uuid in instance_uuids:
        output[instance_uuid] = []

    for row in rows:
        data = dict(row.iteritems())
        output[row['instance_uuid']].append(data)

    return output

def get_latest_for_instance(cls, context, instance_uuid):
        db_faults = instance_fault_get_by_instance_uuids(context,
                                                            [instance_uuid])
        if instance_uuid in db_faults and db_faults[instance_uuid]:
            return cls._from_db_object(context, cls(),
                                       db_faults[instance_uuid][0])

def metadata_to_dict(metadata):
    result = {}
    for item in metadata:
        if not item.get('deleted'):
            result[item['key']] = item['value']
    return result

def instance_meta(instance):
    if isinstance(instance['metadata'], dict):
        return instance['metadata']
    else:
        return metadata_to_dict(instance['metadata'])

def instance_sys_meta(instance):
    if not instance.get('system_metadata'):
        return {}
    if isinstance(instance['system_metadata'], dict):
        return instance['system_metadata']
    else:
        return metadata_to_dict(instance['system_metadata'])

def _from_db_object(context, instance, db_inst, expected_attrs=None):
    """Method to help with migration to objects.

    Converts a database entity to a formal object.
    """
    print("[DEBUG_LOG] instance._from_db_object(%s, %s, %s)" % (instance, db_inst, expected_attrs))
    instance._context = context
    if expected_attrs is None:
        expected_attrs = []
    # Most of the field names match right now, so be quick
    for field in instance.fields:
        if field in INSTANCE_OPTIONAL_ATTRS:
            continue
        elif field == 'deleted':
            instance.deleted = db_inst['deleted'] == db_inst['id']
        elif field == 'cleaned':
            instance.cleaned = db_inst['cleaned'] == 1
        else:
            instance[field] = db_inst[field]

    if 'metadata' in expected_attrs:
        instance['metadata'] = instance_meta(db_inst)
    try:
        if 'system_metadata' in expected_attrs:
            instance['system_metadata'] = instance_sys_meta(db_inst)
    except:
        LOG.error("instance._from_db_object encountered an error with %s" % (db_inst))
        raise
    if 'fault' in expected_attrs:
        instance['fault'] = (
            get_latest_for_instance(
                context, instance.uuid))
    if 'numa_topology' in expected_attrs:
        instance._load_numa_topology()
    if 'pci_requests' in expected_attrs:
        instance._load_pci_requests()
    return
    # if 'info_cache' in expected_attrs:
    #     if db_inst['info_cache'] is None:
    #         instance.info_cache = None
    #     elif not instance.obj_attr_is_set('info_cache'):
    #         # TODO(danms): If this ever happens on a backlevel instance
    #         # passed to us by a backlevel service, things will break
    #         instance.info_cache = objects.InstanceInfoCache(context)
    #     if instance.info_cache is not None:
    #         instance.info_cache._from_db_object(context,
    #                                             instance.info_cache,
    #                                             db_inst['info_cache'])
    #
    # # TODO(danms): If we are updating these on a backlevel instance,
    # # we'll end up sending back new versions of these objects (see
    # # above note for new info_caches
    # if 'pci_devices' in expected_attrs:
    #     pci_devices = obj_make_list(
    #             context, objects.PciDeviceList(context),
    #             objects.PciDevice, db_inst['pci_devices'])
    #     instance['pci_devices'] = pci_devices
    # if 'security_groups' in expected_attrs:
    #     sec_groups = obj_make_list(
    #             context, objects.SecurityGroupList(context),
    #             objects.SecurityGroup, db_inst['security_groups'])
    #     instance['security_groups'] = sec_groups
    #
    # instance.obj_reset_changes()
    # return instance



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

    one_instance = Query(models.Instance).first()
    not one_instance.system_metadata
    coin = ModelInstance()
    _from_db_object(context, coin, one_instance, ['metadata', 'system_metadata', 'info_cache', 'security_groups'])
