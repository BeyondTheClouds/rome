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
from sqlalchemy.orm import joinedload

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


def instance_get_active_by_window_joined(context, begin, end=None,
                                         project_id=None, host=None,
                                         use_slave=False):
    """Return instances and joins that were active during window."""
    session = get_session(use_slave=use_slave)
    query = session.query(models.Instance)

    query = query.options(joinedload('info_cache')).\
                  options(joinedload('security_groups')).\
                  filter(or_(models.Instance.terminated_at == null(),
                             models.Instance.terminated_at > begin))
    if end:
        query = query.filter(models.Instance.launched_at < end)
    if project_id:
        query = query.filter_by(project_id=project_id)
    if host:
        query = query.filter_by(host=host)

    return _instances_fill_metadata(context, query.all())

class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("admin", "admin")

    host = "jonathan-VirtualBox"
    # host = "edel-17"

    filters = {'deleted': True,
                   'soft_deleted': False,
                   'host': host,
                   'cleaned': False}

    # filters = {'deleted': False, 'project_id': u'6bcb3e3fcf2e4d238e22be73215dc394'}

    sort_key='created_at'
    sort_dir='desc'

    attrs = ['info_cache', 'security_groups', 'system_metadata']
    # with utils.temporary_mutation(context, read_deleted='yes'):

    import datetime

    date_start = datetime.datetime(2016, 1, 1, 0, 0)
    date_end = datetime.datetime(2016, 1, 7, 1, 7, 3)
    project_id = u'500a73c9c6364bf3b02bba983a1ca9e2'
    host = None
    instances = instance_get_active_by_window_joined(context, date_start, date_end, project_id, host)
    LOG.debug('There are %d instances to clean', len(instances))

    for instance in instances:
        print(instance)