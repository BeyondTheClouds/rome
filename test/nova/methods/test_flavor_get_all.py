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
from sqlalchemy.sql import true

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
def _flavor_get_query(context, session=None, read_deleted=None):
    query = model_query(context, models.InstanceTypes, session=session,
                       read_deleted=read_deleted)
    if not context.is_admin:
        the_filter = [models.InstanceTypes.is_public == true()]
        the_filter.extend([
            models.InstanceTypes.projects.any(project_id=context.project_id)
        ])
        query = query.filter(or_(*the_filter))
    return query


def _dict_with_extra_specs(inst_type_query):
    """Takes an instance or instance type query returned
    by sqlalchemy and returns it as a dictionary, converting the
    extra_specs entry from a list of dicts:

    'extra_specs' : [{'key': 'k1', 'value': 'v1', ...}, ...]

    to a single dict:

    'extra_specs' : {'k1': 'v1'}

    """
    inst_type_dict = dict(inst_type_query)
    extra_specs = dict([(x['key'], x['value'])
                        for x in inst_type_query['extra_specs']])
    inst_type_dict['extra_specs'] = extra_specs
    return inst_type_dict

def flavor_get_all(context, inactive=False, filters=None,
                   sort_key='flavorid', sort_dir='asc', limit=None,
                   marker=None):
    """Returns all flavors.
    """
    filters = filters or {}

    # FIXME(sirp): now that we have the `disabled` field for flavors, we
    # should probably remove the use of `deleted` to mark inactive. `deleted`
    # should mean truly deleted, e.g. we can safely purge the record out of the
    # database.
    read_deleted = "yes" if inactive else "no"

    query = _flavor_get_query(context, read_deleted=read_deleted)

    if 'min_memory_mb' in filters:
        query = query.filter(
                models.InstanceTypes.memory_mb >= filters['min_memory_mb'])

    if 'min_root_gb' in filters:
        query = query.filter(
                models.InstanceTypes.root_gb >= filters['min_root_gb'])

    if 'disabled' in filters:
        query = query.filter(
                models.InstanceTypes.disabled == filters['disabled'])

    if 'is_public' in filters and filters['is_public'] is not None:
        the_filter = [models.InstanceTypes.is_public == filters['is_public']]
        # if filters['is_public'] and context.project_id is not None:
        #     the_filter.extend([
        #         models.InstanceTypes.projects.any(
        #             project_id=context.project_id, deleted=0)
        #     ])
        if len(the_filter) > 1:
            query = query.filter(or_(*the_filter))
        else:
            query = query.filter(the_filter[0])

    marker_row = None
    if marker is not None:
        marker_row = _flavor_get_query(context, read_deleted=read_deleted).\
                    filter_by(flavorid=marker).\
                    first()
        if not marker_row:
            raise Exception("MarkerNotFound(%s)" % (marker))

    # query = sqlalchemyutils.paginate_query(query, models.InstanceTypes, limit,
    #                                        [sort_key, 'id'],
    #                                        marker=marker_row,
    #                                        sort_dir=sort_dir)

    query = RomeQuery(models.InstanceTypes)
    inst_types = query.all()

    return [_dict_with_extra_specs(i) for i in inst_types]

class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id
        self.is_admin = True


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("admin", "admin")

    print(flavor_get_all(context))
