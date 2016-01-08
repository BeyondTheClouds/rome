__author__ = 'jonathan'

import logging

from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import joinedload

# import test.nova._fixtures as models
import nova.db.discovery.models as models

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

import collections

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


def aggregate_metadata_get_by_metadata_key(context, aggregate_id, key):
    query = model_query(context, models.Aggregate)
    # TODO(jonathan): change following to support ROME convention.
    query = query.join("_metadata")
    query = query.join(models.AggregateMetadata)
    query = query.filter(models.AggregateMetadata.key == key)
    query = query.join(models.AggregateHost)
    # TODO(jonathan): change following to support ROME convention.

    if key:
        query = query.filter(models.AggregateMetadata.key == key)

    rows = map(lambda x: x[0], query.all())

    metadata = collections.defaultdict(set)
    for agg in rows:
        for kv in agg._metadata:
            metadata[kv['key']].add(kv['value'])
    return dict(metadata)

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

    aggregate_metadata_get_by_metadata_key(context, u"1", "availability_zone")