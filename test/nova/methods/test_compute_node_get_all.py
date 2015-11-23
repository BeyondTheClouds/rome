__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

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


def compute_node_get_all(context, no_date_fields):

    # NOTE(msdubov): Using lower-level 'select' queries and joining the tables
    #                manually here allows to gain 3x speed-up and to have 5x
    #                less network load / memory usage compared to the sqla ORM.

    # engine = get_engine()

    # # Retrieve ComputeNode, Service
    # compute_node = models.ComputeNode.__table__
    # service = models.Service.__table__

    # with engine.begin() as conn:
    #     redundant_columns = set(['deleted_at', 'created_at', 'updated_at',
    #                              'deleted']) if no_date_fields else set([])

    #     def filter_columns(table):
    #         return [c for c in table.c if c.name not in redundant_columns]

    #     compute_node_query = sql.select(filter_columns(compute_node)).\
    #                             where(compute_node.c.deleted == 0).\
    #                             order_by(compute_node.c.service_id)
    #     compute_node_rows = conn.execute(compute_node_query).fetchall()

    #     service_query = sql.select(filter_columns(service)).\
    #                         where((service.c.deleted == 0) &
    #                               (service.c.binary == 'nova-compute')).\
    #                         order_by(service.c.id)
    #     service_rows = conn.execute(service_query).fetchall()

    # # Join ComputeNode & Service manually.
    # services = {}
    # for proxy in service_rows:
    #     services[proxy['id']] = dict(proxy.items())

    # compute_nodes = []
    # for proxy in compute_node_rows:
    #     node = dict(proxy.items())
    #     node['service'] = services.get(proxy['service_id'])

    #     compute_nodes.append(node)
    from lib.rome.core.dataformat.json import Encoder
    from lib.rome.core.dataformat.json import Decoder

    query = RomeQuery(models.ComputeNode)
    compute_nodes = query.all()

    def novabase_to_dict(ref):
        request_uuid = uuid.uuid1()
        encoder = Encoder(request_uuid=request_uuid)
        decoder = Decoder(request_uuid=request_uuid)

        json_object = encoder.simplify(ref)
        json_object.pop("_metadata_novabase_classname")

        return decoder.desimplify(json_object)

    # result = []
    # for each in compute_nodes:
    #     compute_node = novabase_to_dict(each)
    #     compute_node["service"] = novabase_to_dict(compute_node["service"])
    #     compute_node["service"].pop("compute_node")
    #     result += [compute_node]

    return compute_nodes


def test_compute_node_get_all():

    context = Context("project1", "user1")

    service_count = Query(models.Service).count()
    service = models.Service()
    service.host = "host1"
    service.binary = "binary1"
    service.topic = "topic1"
    service.report_count = 0
    service.save()

    compute_node_count = Query(models.ComputeNode).count()
    compute_nodes = []
    for i in range(0, 2):
        compute_node = models.ComputeNode()
        compute_node.vcpus = 12
        compute_node.service = service

        compute_node.save()
        compute_nodes += [compute_node]

    assert Query(models.Service).count() == service_count + 1
    assert Query(models.ComputeNode).count() == compute_node_count + len(compute_nodes)

    compute_nodes_linked_to_this_service = Query(models.ComputeNode).filter(models.ComputeNode.service_id==service.id).all()
    for cn in compute_nodes_linked_to_this_service:
        assert cn.service.id == service.id


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

    test_compute_node_get_all()
