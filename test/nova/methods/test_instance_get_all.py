__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

from test.nova.methods.test_ensure_default_secgroup import _security_group_ensure_default, _security_group_get_query
from lib.rome.core.orm.query import or_
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import joinedload_all

import logging
import uuid
# from oslo.utils import timeutils
from lib.rome.core.utils import timeutils
from sqlalchemy.sql.expression import asc
from sqlalchemy.sql.expression import desc

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


def _manual_join_columns(columns_to_join):
    manual_joins = []
    for column in ('metadata', 'system_metadata', 'pci_devices'):
        if column in columns_to_join:
            columns_to_join.remove(column)
            manual_joins.append(column)
    return manual_joins, columns_to_join


def instance_get_all_by_filters(context, filters, sort_key, sort_dir,
                                limit=None, marker=None, columns_to_join=None,
                                use_slave=False):
    """Return instances that match all filters.  Deleted instances
    will be returned by default, unless there's a filter that says
    otherwise.
    Depending on the name of a filter, matching for that filter is
    performed using either exact matching or as regular expression
    matching. Exact matching is applied for the following filters::
    |   ['project_id', 'user_id', 'image_ref',
    |    'vm_state', 'instance_type_id', 'uuid',
    |    'metadata', 'host', 'system_metadata']
    A third type of filter (also using exact matching), filters
    based on instance metadata tags when supplied under a special
    key named 'filter'::
    |   filters = {
    |       'filter': [
    |           {'name': 'tag-key', 'value': '<metakey>'},
    |           {'name': 'tag-value', 'value': '<metaval>'},
    |           {'name': 'tag:<metakey>', 'value': '<metaval>'}
    |       ]
    |   }
    Special keys are used to tweek the query further::
    |   'changes-since' - only return instances updated after
    |   'deleted' - only return (or exclude) deleted instances
    |   'soft_deleted' - modify behavior of 'deleted' to either
    |                    include or exclude instances whose
    |                    vm_state is SOFT_DELETED.
    """
    # NOTE(mriedem): If the limit is 0 there is no point in even going
    # to the database since nothing is going to be returned anyway.
    if limit == 0:
        return []

    sort_fn = {'desc': desc, 'asc': asc}

    # if CONF.database.slave_connection == '':
    #     use_slave = False

    session = get_session(use_slave=use_slave)

    if columns_to_join is None:
        columns_to_join = ['info_cache', 'security_groups']
        manual_joins = ['metadata', 'system_metadata']
    else:
        manual_joins, columns_to_join = _manual_join_columns(columns_to_join)

    query_prefix = session.query(models.Instance)
    for column in columns_to_join:
        query_prefix = query_prefix.options(joinedload(column))

    query_prefix = query_prefix.order_by(sort_fn[sort_dir](
            getattr(models.Instance, sort_key)))

    # Make a copy of the filters dictionary to use going forward, as we'll
    # be modifying it and we shouldn't affect the caller's use of it.
    filters = filters.copy()
    filters_ = {}

    query_prefix = session.query(models.Instance)
    if 'changes-since' in filters:
        filters.pop('changes_since')
        changes_since = timeutils.normalize_time(filters['changes-since'])
        query_prefix = query_prefix.\
                            filter(models.Instance.updated_at >= changes_since)

    if 'deleted' in filters:
        # Instances can be soft or hard deleted and the query needs to
        # include or exclude both
        if filters.pop('deleted'):
            if filters.pop('soft_deleted', True):
                deleted = or_(
                    models.Instance.deleted == models.Instance.id
                    )
                query_prefix = query_prefix.\
                    filter(deleted)
            else:
                query_prefix = query_prefix.\
                    filter(models.Instance.deleted == models.Instance.id)
        else:
            query_prefix = query_prefix.\
                    filter_by(deleted=0)
            if not filters.pop('soft_deleted', False):
                # It would be better to have vm_state not be nullable
                # but until then we test it explicitly as a workaround.
                not_soft_deleted = or_(
                    models.Instance.vm_state != "soft-deleting",
                    models.Instance.vm_state == None
                    )
                query_prefix = query_prefix.filter(not_soft_deleted)

    # if 'cleaned' in filters:
    #     if filters.pop('cleaned'):
    #         query_prefix = query_prefix.filter(models.Instance.cleaned == 1)
    #     else:
    #         query_prefix = query_prefix.filter(models.Instance.cleaned == 0)

    # if not context.is_admin:
    #     # If we're not admin context, add appropriate filter..
    #     if context.project_id:
    #         filters['project_id'] = context.project_id
    #     else:
    #         filters['user_id'] = context.user_id

    # # Filters for exact matches that we can do along with the SQL query...
    # # For other filters that don't match this, we will do regexp matching
    # exact_match_filter_names = ['project_id', 'user_id', 'image_ref',
    #                             'vm_state', 'instance_type_id', 'uuid',
    #                             'metadata', 'host', 'task_state',
    #                             'system_metadata']

    # # Filter the query
    # query_prefix = exact_filter(query_prefix, models.Instance,
    #                             filters, exact_match_filter_names)

    # query_prefix = regex_filter(query_prefix, models.Instance, filters)
    # query_prefix = tag_filter(context, query_prefix, models.Instance,
    #                           models.InstanceMetadata,
    #                           models.InstanceMetadata.instance_uuid,
    #                           filters)

    # paginate query
    # if marker is not None:
    #     try:
    #         marker = _instance_get_by_uuid(context, marker, session=session)
    #     except exception.InstanceNotFound:
    #         raise exception.MarkerNotFound(marker)
    # TODO: following cannot yet work with the RIAK DB implementation!
    # query_prefix = sqlalchemyutils.paginate_query(query_prefix,
    #                        models.Instance, limit,
    #                        [sort_key, 'created_at', 'id'],
    #                        marker=marker,
    #                        sort_dir=sort_dir)
    # print("filters: %s" % (filters))
    # query_prefix = RomeQuery(models.Instance).filter_dict(filters_)
    # query_prefix = RomeQuery(models.Instance)
    return query_prefix.all()
    # return _instances_fill_metadata(context, query_prefix.all(), manual_joins)


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

    instance_get_all_by_filters(context, {'deleted': False, 'project_id': u'3513a03fa34c470c9be71b58e718dcaf'}, 'created_at', 'desc')
