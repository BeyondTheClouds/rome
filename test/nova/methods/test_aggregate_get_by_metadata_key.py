__author__ = 'jonathan'

import logging

from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import joinedload

# import test.nova._fixtures as models
import nova.db.discovery.models as models

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

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

def aggregate_get_by_metadata_key(context, key):
    """Return rows that match metadata key.

    :param key Matches metadata key.
    """
    query = model_query(context, models.Aggregate)
    print("[aggregate_get_by_metadata_key] (1) result:%s" % (query.all()))
    # TODO(jonathan): change following to support ROME convention.
    # query = query.join("_metadata")
    query = query.join(models.AggregateMetadata)
    print("[aggregate_get_by_metadata_key] (2) result:%s" % (query.all()))
    query = query.filter(models.AggregateMetadata.key == key)
    print("[aggregate_get_by_metadata_key] (3) result:%s" % (query.all()))
    # query = query.options(contains_eager("_metadata"))
    # query = query.options(joinedload("_hosts"))
    # # TODO(jonathan): change following to support ROME convention.
    # # return query.all()
    result = query.all()
    print("[aggregate_get_by_metadata_key] result:%s" % (result))
    processed_result = map(lambda x: x[0], query.all())
    processed_result = map(lambda x: x.get_complex_ref(), processed_result)
    print("[aggregate_get_by_metadata_key] processed_result:%s" % (processed_result))
    # processed_result = []
    return processed_result


def _aggregate_get_query(context, model_class, id_field=None, id=None,
                         session=None, read_deleted=None):
    columns_to_join = {models.Aggregate: ['_hosts', '_metadata']}

    query = model_query(context, model_class, session=session,
                        read_deleted=read_deleted)

    for c in columns_to_join.get(model_class, []):
        query = query.options(joinedload(c))

    if id and id_field:
        query = query.filter(id_field == id)

    return query

def aggregate_get(context, aggregate_id):
    print("[aggregate_get] id:%s" % (aggregate_id))
    query = _aggregate_get_query(context,
                                 models.Aggregate,
                                 models.Aggregate.id,
                                 aggregate_id)
    aggregate = query.first()
    print("[aggregate_get] aggregate:%s" % (aggregate))

    if not aggregate:
        raise Exception("aggregate %s not found :(" % (aggregate_id))

    return aggregate

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

    context = Context("admin", "admin")

    # result = aggregate_get_by_metadata_key(context, "availability_zone")
    #
    # for each in result:
    #     aggregate_id = each.id
    aggregate_id = 1
    plop = aggregate_get(context, "%s" % (aggregate_id))
    plop = aggregate_get(context, '%s' % (aggregate_id))
    plop = aggregate_get(context, aggregate_id)
    print(plop)