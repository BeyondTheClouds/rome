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
import collections

from sqlalchemy.sql.expression import asc
from sqlalchemy.sql.expression import desc
from nova.compute import vm_states

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

def _instance_get_all_query(context, project_only=False,
                            joins=None, use_slave=False):
    if joins is None:
        joins = ['info_cache', 'security_groups']

    query = model_query(context,
                        models.Instance,
                        project_only=project_only,
                        use_slave=use_slave)
    # for join in joins:
    #     query = query.options(joinedload(join))
    return query

def _instance_pcidevs_get_multi(context, instance_uuids, session=None):
    return model_query(context, models.PciDevice, session=session).\
        filter_by(status='allocated').\
        filter(models.PciDevice.instance_uuid.in_(instance_uuids))

def _instances_fill_metadata(context, instances,
                             manual_joins=None, use_slave=False):
    """Selectively fill instances with manually-joined metadata. Note that
    instance will be converted to a dict.
    :param context: security context
    :param instances: list of instances to fill
    :param manual_joins: list of tables to manually join (can be any
                         combination of 'metadata' and 'system_metadata' or
                         None to take the default of both)
    """

    def flatten(l):
        return [item for sublist in l for item in sublist]

    uuids = [inst['uuid'] for inst in instances]

    if manual_joins is None:
        manual_joins = ['metadata', 'system_metadata']

    meta = collections.defaultdict(list)
    if 'system_metadata' in manual_joins:
        for instance in instances:
            for metadata in instance.metadata:
                meta[instance.uuid].append(metadata)

    sys_meta = collections.defaultdict(list)
    if 'system_metadata' in manual_joins:
        for instance in instances:
            for system_metadata in instance.system_metadata:
                sys_meta[instance.uuid].append(system_metadata)

    pcidevs = collections.defaultdict(list)
    if 'pci_devices' in manual_joins:
        for row in _instance_pcidevs_get_multi(context, uuids):
            pcidevs[row['instance_uuid']].append(row)

    filled_instances = []
    for inst in instances:
        inst = dict(inst.iteritems())
        # inst['system_metadata'] = sys_meta[inst['uuid']]
        inst['metadata'] = meta[inst['uuid']]
        if 'pci_devices' in manual_joins:
            inst['pci_devices'] = pcidevs[inst['uuid']]
        filled_instances.append(inst)

    return filled_instances

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
    # for column in columns_to_join:
    #     query_prefix = query_prefix.options(joinedload(column))

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
                    models.Instance.deleted == models.Instance.id,
                    models.Instance.vm_state == vm_states.SOFT_DELETED
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
                    models.Instance.vm_state != vm_states.SOFT_DELETED,
                    models.Instance.vm_state == null()
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


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("admin", "admin")

    host = "jonathan-VirtualBox"
    # host = "edel-17"

    # filters = {'deleted': True,
    #                'soft_deleted': False,
    #                'host': host,
    #                'cleaned': False}

    filters = {'deleted': False, 'project_id': u'6bcb3e3fcf2e4d238e22be73215dc394'}

    sort_key='created_at'
    sort_dir='desc'

    attrs = ['info_cache', 'security_groups', 'system_metadata']
    # with utils.temporary_mutation(context, read_deleted='yes'):
    instances = instance_get_all_by_filters(context, filters, sort_key, sort_dir)
    LOG.debug('There are %d instances to clean', len(instances))

    for instance in instances:
        print(instance)