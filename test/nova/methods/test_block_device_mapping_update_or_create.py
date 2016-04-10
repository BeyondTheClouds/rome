__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

from test.nova.methods.test_ensure_default_secgroup import _security_group_ensure_default, _security_group_get_query
from lib.rome.core.orm.query import or_

import logging
import uuid
# from oslo.utils import timeutils
from lib.rome.core.utils import timeutils
from sqlalchemy.sql.expression import asc
from sqlalchemy.sql.expression import desc
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import joinedload_all

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

def _scrub_empty_str_values(dct, keys_to_scrub):
    """Remove any keys found in sequence keys_to_scrub from the dict
    if they have the value ''.
    """
    for key in keys_to_scrub:
        if key in dct and dct[key] == '':
            del dct[key]

def _block_device_mapping_get_query(context, columns_to_join=None):
    if columns_to_join is None:
        columns_to_join = []

    query = model_query(context, models.BlockDeviceMapping)

    for column in columns_to_join:
        query = query.options(joinedload(column))

    return query

def _from_legacy_values(values, legacy, allow_updates=False):
    if legacy:
        if allow_updates and block_device.is_safe_for_update(values):
            return values
        else:
            return block_device.BlockDeviceDict.from_legacy(values)
    else:
        return values

def convert_objects_related_datetimes(values, *datetime_keys):
    if not datetime_keys:
        datetime_keys = ('created_at', 'deleted_at', 'updated_at')

    for key in datetime_keys:
        if key in values and values[key]:
            if isinstance(values[key], six.string_types):
                try:
                    values[key] = timeutils.parse_strtime(values[key])
                except ValueError:
                    # Try alternate parsing since parse_strtime will fail
                    # with say converting '2015-05-28T19:59:38+00:00'
                    values[key] = timeutils.parse_isotime(values[key])
            # NOTE(danms): Strip UTC timezones from datetimes, since they're
            # stored that way in the database
            values[key] = values[key].replace(tzinfo=None)
    return values

def block_device_mapping_update_or_create(context, values, legacy=True):
    _scrub_empty_str_values(values, ['volume_size'])
    values = _from_legacy_values(values, legacy, allow_updates=True)
    convert_objects_related_datetimes(values)

    result = None
    # NOTE(xqueralt): Only update a BDM when device_name was provided. We
    # allow empty device names so they will be set later by the manager.
    if values['device_name']:
        query = _block_device_mapping_get_query(context)
        result = query.filter_by(instance_uuid=values['instance_uuid'],
                                 device_name=values['device_name']).first()

    if result:
        result.update(values)
    else:
        # Either the device_name doesn't exist in the database yet, or no
        # device_name was provided. Both cases mean creating a new BDM.
        result = models.BlockDeviceMapping()
        result.update(values)
        result.save(context.session)

    # NOTE(xqueralt): Prevent from having multiple swap devices for the
    # same instance. This will delete all the existing ones.
    if block_device.new_format_is_swap(values):
        query = _block_device_mapping_get_query(context)
        query = query.filter_by(instance_uuid=values['instance_uuid'],
                                source_type='blank', guest_format='swap')
        query = query.filter(models.BlockDeviceMapping.id != result.id)
        query.soft_delete()

    return result

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

    values = {"device_name": "block-device-1", "image_id": 1, "instance_uuid": "uuid_1"}
    block_device_mapping_update_or_create(context, values, False)
