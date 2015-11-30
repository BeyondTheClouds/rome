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

def _flavor_get_query(context, session=None, read_deleted=None):
    query = model_query(context, models.InstanceTypes, session=session,
                       read_deleted=read_deleted)
    if not context.is_admin:
        the_filter = [models.InstanceTypes.is_public == True]
        the_filter.extend([
            models.InstanceTypes.projects.any(project_id=context.project_id)
        ])
        query = query.filter(or_(*the_filter))
    return query

def flavor_get_by_flavor_id(context, flavor_id, read_deleted):
    """Returns a dict describing specific flavor_id."""
    result = _flavor_get_query(context, read_deleted=read_deleted).\
                        filter_by(flavorid=flavor_id).\
                        first()
    if not result:
        raise Exception("plop")
    return _dict_with_extra_specs(result)


class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id
        self.is_admin = True


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("admin", "admin")

    print(flavor_get_by_flavor_id(context, '42', 'no'))
