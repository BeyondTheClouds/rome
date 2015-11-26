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


def security_group_ensure_default(context):
    """Ensure default security group exists for a project_id."""

    try:
        return _security_group_ensure_default(context)
    except Exception:
        # NOTE(rpodolyaka): a concurrent transaction has succeeded first,
        # suppress the error and proceed
        pass

def _security_group_get_by_names(context, session, project_id, group_names):
    """Get security group models for a project by a list of names.
    Raise SecurityGroupNotFoundForProject for a name not found.
    """
    query = _security_group_get_query(context, session=session,
                                      read_deleted="no", join_rules=False).\
            filter_by(project_id=project_id).\
            filter(models.SecurityGroup.name.in_(group_names))
    sg_models = query.all()
    if len(sg_models) == len(group_names):
        return sg_models
    # Find the first one missing and raise
    group_names_from_models = [x.name for x in sg_models]
    for group_name in group_names:
        if group_name not in group_names_from_models:
            raise Exception()
    # Not Reached


def _metadata_refs(metadata_dict, meta_class):
    metadata_refs = []
    if metadata_dict:
        for k, v in metadata_dict.iteritems():
            metadata_ref = meta_class()
            metadata_ref['key'] = k
            metadata_ref['value'] = v
            metadata_refs.append(metadata_ref)
    return metadata_refs

# def convert_objects_related_datetimes(values, *datetime_keys):
#     for key in datetime_keys:
#         if key in values and values[key]:
#             # if isinstance(values[key], six.string_types):
#             #     values[key] = timeutils.parse_strtime(values[key])
#             # NOTE(danms): Strip UTC timezones from datetimes, since they're
#             # stored that way in the database
#             values[key] = values[key].replace(tzinfo=None)
#     return values

def convert_objects_related_datetimes(values, *datetime_keys):
    for key in datetime_keys:
        if key in values and values[key]:
            if isinstance(values[key], six.string_types):
                values[key] = timeutils.parse_strtime(values[key])
            # NOTE(danms): Strip UTC timezones from datetimes, since they're
            # stored that way in the database
            values[key] = values[key].replace(tzinfo=None)
    return values

def _handle_objects_related_type_conversions(values):
    """Make sure that certain things in values (which may have come from
    an objects.instance.Instance object) are in suitable form for the
    database.
    """
    # NOTE(danms): Make sure IP addresses are passed as strings to
    # the database engine
    for key in ('access_ip_v4', 'access_ip_v6'):
        if key in values and values[key] is not None:
            values[key] = str(values[key])

    datetime_keys = ('created_at', 'deleted_at', 'updated_at',
                     'launched_at', 'terminated_at', 'scheduled_at')
    convert_objects_related_datetimes(values, *datetime_keys)

def _validate_unique_server_name(context, session, name):
    return

def ec2_instance_create(context, instance_uuid, id=None):
    """Create ec2 compatible instance by provided uuid."""
    ec2_instance_ref = models.InstanceIdMapping()
    ec2_instance_ref.update({'uuid': instance_uuid}, do_save=False)
    if id is not None:
        ec2_instance_ref.update({'id': id}, do_save=False)

    ec2_instance_ref.save()

    return ec2_instance_ref


def instance_create(context, values):
    """Create a new Instance record in the database.

    context - request context object
    values - dict containing column values.
    """

    # NOTE(rpodolyaka): create the default security group, if it doesn't exist.
    # This must be done in a separate transaction, so that this one is not
    # aborted in case a concurrent one succeeds first and the unique constraint
    # for security group names is violated by a concurrent INSERT
    security_group_ensure_default(context)

    values = values.copy()
    values['metadata'] = _metadata_refs(
            values.get('metadata'), models.InstanceMetadata)

    values['system_metadata'] = _metadata_refs(
            values.get('system_metadata'), models.InstanceSystemMetadata)
    _handle_objects_related_type_conversions(values)

    instance_ref = models.Instance()
    if not values.get('uuid'):
        values['uuid'] = str(uuid.uuid4())
    instance_ref['info_cache'] = models.InstanceInfoCache()
    info_cache = values.pop('info_cache', None)
    if info_cache is not None:
        instance_ref['info_cache'].update(info_cache)
    security_groups = values.pop('security_groups', [])
    instance_ref.update(values, do_save=False)

    # TODO(jonathan): explicitly set the uuid for instance's metadata
    for metadata in values.get("metadata"):
        metadata.uuid = values["uuid"]
    for metadata in values.get("system_metadata"):
        metadata.uuid = values["uuid"]

    def _get_sec_group_models(session, security_groups):
        models = []
        default_group = _security_group_ensure_default(context, session)
        if 'default' in security_groups:
            models.append(default_group)
            # Generate a new list, so we don't modify the original
            security_groups = [x for x in security_groups if x != 'default']
        if security_groups:
            models.extend(_security_group_get_by_names(context,
                    session, context.project_id, security_groups))
        return models

    session = get_session()
    with session.begin():
        if 'hostname' in values:
            _validate_unique_server_name(context, session, values['hostname'])
        instance_ref.security_groups = _get_sec_group_models(session,
                security_groups)
        session.add(instance_ref)

    # create the instance uuid to ec2_id mapping entry for instance
    ec2_instance_create(context, instance_ref['uuid'])

    _instance_extra_create(context, {'instance_uuid': instance_ref['uuid']})

    return instance_ref

def _instance_extra_create(context, values):
    inst_extra_ref = models.InstanceExtra()
    inst_extra_ref.update(values)
    inst_extra_ref.save()
    return inst_extra_ref


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

    # values = {'vm_state': u'building', 'availability_zone': None, 'ephemeral_gb': 0, 'instance_type_id': 1, 'user_data': None, 'vm_mode': None, 'reservation_id': u'r-efuqt0e0', 'security_groups': [u'default'], 'root_device_name': None, 'user_id': u'4249791567dd4331807f1d0366eee23e', 'uuid': 'ad14ea73-219d-47a5-95b1-862c6c2b5559', 'info_cache': {'network_info': '[]'}, 'hostname': u'vm-node-1-0', 'display_description': u'vm_node_1_0', 'key_data': None, 'power_state': 0, 'progress': 0, 'project_id': u'93ae28587bb04f5a99a942883e9ca0bf', 'metadata': {}, 'ramdisk_id': u'f04ed562-8dc7-4dab-8834-a1f3b459c3cb', 'access_ip_v6': None, 'access_ip_v4': None, 'kernel_id': u'25b24691-323d-4eee-bfaa-45d25fcf9c66', 'key_name': None, 'ephemeral_key_uuid': None, 'display_name': u'vm_node_1_0', 'system_metadata': {u'image_kernel_id': u'25b24691-323d-4eee-bfaa-45d25fcf9c66', 'instance_type_memory_mb': u'64', 'instance_type_swap': u'0', 'instance_type_vcpu_weight': None, 'instance_type_root_gb': u'0', 'instance_type_id': u'1', u'image_ramdisk_id': u'f04ed562-8dc7-4dab-8834-a1f3b459c3cb', 'instance_type_name': u'm1.nano', 'instance_type_ephemeral_gb': u'0', 'instance_type_rxtx_factor': u'1.0', 'image_disk_format': u'ami', 'instance_type_flavorid': u'42', 'image_container_format': u'ami', 'instance_type_vcpus': u'1', 'image_min_ram': 0, 'image_min_disk': 0, 'image_base_image_ref': u'124f2f1a-a92d-4617-8f32-c3fb9147b4c0'}, 'task_state': u'scheduling', 'shutdown_terminate': False, 'root_gb': 0, 'locked': False, 'launch_index': 0, 'memory_mb': 64, 'vcpus': 1, 'image_ref': u'124f2f1a-a92d-4617-8f32-c3fb9147b4c0', 'architecture': None, 'auto_disk_config': False, 'os_type': None, 'config_drive': u''}
    # values = {'vm_state': u'building', 'availability_zone': None, 'ephemeral_gb': 0, 'instance_type_id': 1, 'user_data': None, 'vm_mode': None, 'reservation_id': u'r-efuqt0e0', 'security_groups': [u'default'], 'root_device_name': None, 'user_id': u'4249791567dd4331807f1d0366eee23e', 'uuid': None, 'info_cache': {'network_info': '[]'}, 'hostname': u'vm-node-1-0', 'display_description': u'vm_node_1_0', 'key_data': None, 'power_state': 0, 'progress': 0, 'project_id': u'93ae28587bb04f5a99a942883e9ca0bf', 'metadata': {}, 'ramdisk_id': u'f04ed562-8dc7-4dab-8834-a1f3b459c3cb', 'access_ip_v6': None, 'access_ip_v4': None, 'kernel_id': u'25b24691-323d-4eee-bfaa-45d25fcf9c66', 'key_name': None, 'ephemeral_key_uuid': None, 'display_name': u'vm_node_1_0', 'system_metadata': {u'image_kernel_id': u'25b24691-323d-4eee-bfaa-45d25fcf9c66', 'instance_type_memory_mb': u'64', 'instance_type_swap': u'0', 'instance_type_vcpu_weight': None, 'instance_type_root_gb': u'0', 'instance_type_id': u'1', u'image_ramdisk_id': u'f04ed562-8dc7-4dab-8834-a1f3b459c3cb', 'instance_type_name': u'm1.nano', 'instance_type_ephemeral_gb': u'0', 'instance_type_rxtx_factor': u'1.0', 'image_disk_format': u'ami', 'instance_type_flavorid': u'42', 'image_container_format': u'ami', 'instance_type_vcpus': u'1', 'image_min_ram': 0, 'image_min_disk': 0, 'image_base_image_ref': u'124f2f1a-a92d-4617-8f32-c3fb9147b4c0'}, 'task_state': u'scheduling', 'shutdown_terminate': False, 'root_gb': 0, 'locked': False, 'launch_index': 0, 'memory_mb': 64, 'vcpus': 1, 'image_ref': u'124f2f1a-a92d-4617-8f32-c3fb9147b4c0', 'architecture': None, 'auto_disk_config': False, 'os_type': None, 'config_drive': u''}
    values = {'vm_state': u'building', 'availability_zone': u'nova', 'ephemeral_gb': 0, 'instance_type_id': 1, 'user_data': None, 'vm_mode': None, 'reservation_id': u'r-wtp8yf09', 'security_groups': [u'default'], 'root_device_name': None, 'user_id': u'9896d2549d9d4487bdfaceabb40d8a42', 'uuid': '65ddcf72-7279-44d4-b8ac-fdc48d8633fc', 'info_cache': {'network_info': '[]'}, 'hostname': u'plop', 'display_description': u'plop', 'key_data': None, 'power_state': 0, 'progress': 0, 'project_id': u'54e4c7b651904240a87b79ca19954730', 'metadata': {}, 'ramdisk_id': u'f4734d93-3939-4ab7-9675-a94c11360b4f', 'access_ip_v6': None, 'access_ip_v4': None, 'kernel_id': u'4cb9d8a6-c00d-44b3-8c55-8da57284bc47', 'key_name': None, 'ephemeral_key_uuid': None, 'display_name': u'plop', 'system_metadata': {u'image_kernel_id': u'4cb9d8a6-c00d-44b3-8c55-8da57284bc47', 'instance_type_memory_mb': u'64', 'instance_type_swap': u'0', 'instance_type_vcpu_weight': None, 'instance_type_root_gb': u'0', 'instance_type_id': u'1', u'image_ramdisk_id': u'f4734d93-3939-4ab7-9675-a94c11360b4f', 'instance_type_name': u'm1.nano', 'instance_type_ephemeral_gb': u'0', 'instance_type_rxtx_factor': u'1.0', 'image_disk_format': u'ami', 'instance_type_flavorid': u'42', 'image_container_format': u'ami', 'instance_type_vcpus': u'1', 'image_min_ram': 0, 'image_min_disk': 0, 'image_base_image_ref': u'3fede984-cca6-4d03-829a-9d79ee15d064'}, 'task_state': u'scheduling', 'shutdown_terminate': False, 'root_gb': 0, 'locked': False, 'launch_index': 0, 'memory_mb': 64, 'vcpus': 1, 'image_ref': u'3fede984-cca6-4d03-829a-9d79ee15d064', 'architecture': None, 'auto_disk_config': True, 'os_type': None, 'config_drive': u''}

    instance_create(context, values)
