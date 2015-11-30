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
from oslo.utils import timeutils
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

import random
from nova.openstack.common import uuidutils

from lib.rome.driver.redis.lock import ClusterLock as ClusterLock

dlm = ClusterLock()

def acquire_lock(lockname):
    global dlm
    try_to_lock = True
    while try_to_lock:
        if dlm.lock(lockname, 100):
            fo = open("/opt/logs/db_api.log", "a")
            fo.write("[NET] acquired lock: %s\n" % (lockname))
            fo.close()
            return True

def release_lock(lockname):
    global dlm
    dlm.unlock(lockname)

def fixed_ip_associate_pool(context, network_id, instance_uuid=None,
                            host=None):
    if instance_uuid and not uuidutils.is_uuid_like(instance_uuid):
        raise Exception()
    fo = open("/opt/logs/db_api.log", "a")
    fo.write("[NET] api.fixed_ip_associate_pool() (1-a): network_id: %s\n" % (str(network_id)))
    session = get_session()
    # lockname = "lock-fixed_ip_associate_pool"
    # acquire_lock(lockname)
    fixed_ip_ref_is_none = False
    fixed_ip_ref_instance_uuid_is_not_none = False
    fixed_ip_ref_no_more = False
    with session.begin():
        network_or_none = or_(models.FixedIp.network_id == network_id,
                              models.FixedIp.network_id == None)
        fixed_ips = model_query(context, models.FixedIp, session=session,
                                   read_deleted="no").\
                               filter(network_or_none).\
                               filter_by(reserved=False).\
                               filter_by(instance_uuid=None).\
                               filter_by(host=None).\
                               with_lockmode('update').\
                               all()
        fixed_ip_ref = random.choice(fixed_ips)
        # NOTE(vish): if with_lockmode isn't supported, as in sqlite,
        #             then this has concurrency issues
        if not fixed_ip_ref:
            fixed_ip_ref_no_more = True
        else:
            acquire_lock("lock-fixed_ip_%s" % (fixed_ip_ref.address))
            if fixed_ip_ref['network_id'] is None:
                fixed_ip_ref['network'] = network_id

            if instance_uuid:
                fixed_ip_ref['instance_uuid'] = instance_uuid

            if host:
                fixed_ip_ref['host'] = host
            session.add(fixed_ip_ref)
    # give 100ms to the session to commit changes; then the lock is released.
    # time.sleep(0.1)
    # release_lock(lockname)
    if fixed_ip_ref_no_more:
            raise Exception(net=network_id)
    fo.write("[NET] api.fixed_ip_associate_pool() (1-c): return: %s\n" % (fixed_ip_ref))
    fo.close()
    return fixed_ip_ref

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

    print(fixed_ip_associate_pool(context, 1, None, None))
