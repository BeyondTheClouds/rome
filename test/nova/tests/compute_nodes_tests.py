__author__ = 'jonathan'

from misc import ModelsObjectComparatorMixin

from nova import test
from oslo.serialization import jsonutils
from test.nova import _fixtures as models

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession
from sqlalchemy.sql import false

from sqlalchemy.sql import func
from nova import exception
from nova import db
import copy
import unittest

from nova import context
from oslo.config import cfg
CONF = cfg.CONF

from lib.rome.core.orm.query import Query
import test.nova._fixtures as models
class ComputeNodeTestCase(test.TestCase, ModelsObjectComparatorMixin):

    _ignored_keys = ['id', 'deleted', 'deleted_at', 'created_at', 'updated_at']

    def setUp(self):
        map(lambda x: x.delete(), Query(models.Service).all())
        map(lambda x: x.delete(), Query(models.ComputeNode).all())
        super(ComputeNodeTestCase, self).setUp()
        self.ctxt = context.get_admin_context()
        self.service_dict = dict(host='host1', binary='nova-compute',
                            topic=CONF.compute_topic, report_count=1,
                            disabled=False)
        self.service = db.service_create(self.ctxt, self.service_dict)
        self.compute_node_dict = dict(vcpus=2, memory_mb=1024, local_gb=2048,
                                 vcpus_used=0, memory_mb_used=0,
                                 local_gb_used=0, free_ram_mb=1024,
                                 free_disk_gb=2048, hypervisor_type="xen",
                                 hypervisor_version=1, cpu_info="",
                                 running_vms=0, current_workload=0,
                                 service_id=self.service['id'],
                                 disk_available_least=100,
                                 hypervisor_hostname='abracadabra104',
                                 host_ip='127.0.0.1',
                                 supported_instances='',
                                 pci_stats='',
                                 metrics='',
                                 extra_resources='',
                                 stats='', numa_topology='')
        # add some random stats
        self.stats = dict(num_instances=3, num_proj_12345=2,
                     num_proj_23456=2, num_vm_building=3)
        self.compute_node_dict['stats'] = jsonutils.dumps(self.stats)
        # self.flags(reserved_host_memory_mb=0)
        # self.flags(reserved_host_disk_mb=0)
        self.item = db.compute_node_create(self.ctxt, self.compute_node_dict)

    # def test_compute_node_create(self):
    #     self._assertEqualObjects(self.compute_node_dict, self.item,
    #                             ignored_keys=self._ignored_keys + ['stats'])
    #     new_stats = jsonutils.loads(self.item['stats'])
    #     self.assertEqual(self.stats, new_stats)

    def test_compute_node_get_all(self):
        date_fields = set(['created_at', 'updated_at',
                           'deleted_at', 'deleted'])
        for no_date_fields in [False, True]:
            nodes = db.compute_node_get_all(self.ctxt, no_date_fields)
            self.assertEqual(1, len(nodes))
            node = nodes[0]
            self._assertEqualObjects(self.compute_node_dict, node,
                                     ignored_keys=self._ignored_keys +
                                                  ['stats', 'service'])
            node_fields = set(node.keys())
            if no_date_fields:
                self.assertFalse(date_fields & node_fields)
            else:
                self.assertTrue(date_fields <= node_fields)
            new_stats = jsonutils.loads(node['stats'])
            self.assertEqual(self.stats, new_stats)

    # def test_compute_node_get_all_deleted_compute_node(self):
    #     # Create a service and compute node and ensure we can find its stats;
    #     # delete the service and compute node when done and loop again
    #     for x in range(2, 5):
    #         # Create a service
    #         service_data = self.service_dict.copy()
    #         service_data['host'] = 'host-%s' % x
    #         service = db.service_create(self.ctxt, service_data)
    #
    #         # Create a compute node
    #         compute_node_data = self.compute_node_dict.copy()
    #         compute_node_data['service_id'] = service['id']
    #         compute_node_data['stats'] = jsonutils.dumps(self.stats.copy())
    #         compute_node_data['hypervisor_hostname'] = 'hypervisor-%s' % x
    #         node = db.compute_node_create(self.ctxt, compute_node_data)
    #
    #         # Ensure the "new" compute node is found
    #         nodes = db.compute_node_get_all(self.ctxt, False)
    #         self.assertEqual(2, len(nodes))
    #         found = None
    #         for n in nodes:
    #             if n['id'] == node['id']:
    #                 found = n
    #                 break
    #         self.assertIsNotNone(found)
    #         # Now ensure the match has stats!
    #         self.assertNotEqual(jsonutils.loads(found['stats']), {})
    #
    #         # Now delete the newly-created compute node to ensure the related
    #         # compute node stats are wiped in a cascaded fashion
    #         db.compute_node_delete(self.ctxt, node['id'])
    #
    #         # Clean up the service
    #         db.service_destroy(self.ctxt, service['id'])
    #
    # def test_compute_node_get_all_mult_compute_nodes_one_service_entry(self):
    #     service_data = self.service_dict.copy()
    #     service_data['host'] = 'host2'
    #     service = db.service_create(self.ctxt, service_data)
    #
    #     existing_node = dict(self.item.iteritems())
    #     existing_node['service'] = dict(self.service.iteritems())
    #     expected = [existing_node]
    #
    #     for name in ['bm_node1', 'bm_node2']:
    #         compute_node_data = self.compute_node_dict.copy()
    #         compute_node_data['service_id'] = service['id']
    #         compute_node_data['stats'] = jsonutils.dumps(self.stats)
    #         compute_node_data['hypervisor_hostname'] = 'bm_node_1'
    #         node = db.compute_node_create(self.ctxt, compute_node_data)
    #
    #         node = dict(node.iteritems())
    #         node['service'] = dict(service.iteritems())
    #
    #         expected.append(node)
    #
    #     result = sorted(db.compute_node_get_all(self.ctxt, False),
    #                     key=lambda n: n['hypervisor_hostname'])
    #
    #     self._assertEqualListsOfObjects(expected, result,
    #                                     ignored_keys=['stats'])
    #
    # def test_compute_node_get(self):
    #     compute_node_id = self.item['id']
    #     node = db.compute_node_get(self.ctxt, compute_node_id)
    #     self._assertEqualObjects(self.compute_node_dict, node,
    #                     ignored_keys=self._ignored_keys + ['stats', 'service'])
    #     new_stats = jsonutils.loads(node['stats'])
    #     self.assertEqual(self.stats, new_stats)
    #
    # def test_compute_node_update(self):
    #     compute_node_id = self.item['id']
    #     stats = jsonutils.loads(self.item['stats'])
    #     # change some values:
    #     stats['num_instances'] = 8
    #     stats['num_tribbles'] = 1
    #     values = {
    #         'vcpus': 4,
    #         'stats': jsonutils.dumps(stats),
    #     }
    #     item_updated = db.compute_node_update(self.ctxt, compute_node_id,
    #                                           values)
    #     self.assertEqual(4, item_updated['vcpus'])
    #     new_stats = jsonutils.loads(item_updated['stats'])
    #     self.assertEqual(stats, new_stats)
    #
    # def test_compute_node_delete(self):
    #     compute_node_id = self.item['id']
    #     db.compute_node_delete(self.ctxt, compute_node_id)
    #     nodes = db.compute_node_get_all(self.ctxt)
    #     self.assertEqual(len(nodes), 0)
    #
    # def test_compute_node_search_by_hypervisor(self):
    #     nodes_created = []
    #     new_service = copy.copy(self.service_dict)
    #     for i in xrange(3):
    #         new_service['binary'] += str(i)
    #         new_service['topic'] += str(i)
    #         service = db.service_create(self.ctxt, new_service)
    #         self.compute_node_dict['service_id'] = service['id']
    #         self.compute_node_dict['hypervisor_hostname'] = 'testhost' + str(i)
    #         self.compute_node_dict['stats'] = jsonutils.dumps(self.stats)
    #         node = db.compute_node_create(self.ctxt, self.compute_node_dict)
    #         nodes_created.append(node)
    #     nodes = db.compute_node_search_by_hypervisor(self.ctxt, 'host')
    #     self.assertEqual(3, len(nodes))
    #     self._assertEqualListsOfObjects(nodes_created, nodes,
    #                     ignored_keys=self._ignored_keys + ['stats', 'service'])
    #
    # def test_compute_node_statistics(self):
    #     stats = db.compute_node_statistics(self.ctxt)
    #     self.assertEqual(stats.pop('count'), 1)
    #     for k, v in stats.iteritems():
    #         self.assertEqual(v, self.item[k])
    #
    # def test_compute_node_statistics_disabled_service(self):
    #     serv = db.service_get_by_host_and_topic(
    #         self.ctxt, 'host1', CONF.compute_topic)
    #     db.service_update(self.ctxt, serv['id'], {'disabled': True})
    #     stats = db.compute_node_statistics(self.ctxt)
    #     self.assertEqual(stats.pop('count'), 0)
    #
    # def test_compute_node_not_found(self):
    #     self.assertRaises(exception.ComputeHostNotFound, db.compute_node_get,
    #                       self.ctxt, 100500)
    #
    # def test_compute_node_update_always_updates_updated_at(self):
    #     item_updated = db.compute_node_update(self.ctxt,
    #             self.item['id'], {})
    #     self.assertNotEqual(self.item['updated_at'],
    #                              item_updated['updated_at'])
    #
    # def test_compute_node_update_override_updated_at(self):
    #     # Update the record once so updated_at is set.
    #     first = db.compute_node_update(self.ctxt, self.item['id'],
    #                                    {'free_ram_mb': '12'})
    #     self.assertIsNotNone(first['updated_at'])
    #
    #     # Update a second time. Make sure that the updated_at value we send
    #     # is overridden.
    #     second = db.compute_node_update(self.ctxt, self.item['id'],
    #                                     {'updated_at': first.updated_at,
    #                                      'free_ram_mb': '13'})
    #     self.assertNotEqual(first['updated_at'], second['updated_at'])

if __name__ == "__main__":
    unittest.main()