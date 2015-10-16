import lib.rome.driver.database_driver
import redis
# import json
import rediscluster
from lib.rome.conf.Configuration import get_config
# from redlock import RedLock as RedLock
# import redis_lock
from redlock import Redlock as Redlock
import time

from lib.rome.driver.redis.lock import ClusterLock as ClusterLock

class RedisDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        self.redis_client = redis.StrictRedis(host=config.host(), port=config.port(), db=0)
        self.dlm = Redlock([{"host": "localhost", "port": 6379, "db": 0}, ], retry_count=10)
        # self.dlm = ClusterLock()

    def add_key(self, tablename, key):
        """"""
        pass

    def remove_key(self, tablename, key):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        self.redis_client.hdel(tablename, redis_key)
        pass

    def next_key(self, tablename):
        """"""
        next_key = self.redis_client.incr("nextkey:%s" % (tablename), 1)
        return next_key

    def keys(self, tablename):
        """"""
        """Check if the current table contains keys."""
        keys = self.redis_client.hkeys(tablename)
        return sorted(keys)

    def put(self, tablename, key, value, secondary_indexes=[]):
        """"""
        # lockname = "lock-%s" % (tablename)
        # my_lock = None
        # try_to_lock = True
        # while try_to_lock:
        #     # my_lock = self.dlm.lock(lockname,1000)
        #     my_lock = self.dlm.lock(lockname, 200)
        #     if my_lock is not False:
        #         try_to_lock = False
        #     else:
        #         time.sleep(0.2)
        # json_value = json.dumps(value)
        json_value = value
        fetched = self.redis_client.hset(tablename, "%s:id:%s" % (tablename, key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.redis_client.sadd("sec_index:%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
            # fetched = self.redis_client.hset("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
        result = value if fetched else None
        # self.dlm.unlock(lockname)
        # self.dlm.unlock(my_lock)
        return result

    def get(self, tablename, key, hint=None):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        if hint is not None:
            redis_keys = self.redis_client.smembers("sec_index:%s:%s:%s" % (tablename, hint[0], hint[1]))
            redis_key = redis_keys[0]
            # redis_key = self.redis_client.hget("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, hint[0], hint[1]))
        fetched = self.redis_client.hget(tablename, redis_key)
        # result = json.loads(fetched) if fetched is not None else None
        result = eval(fetched) if fetched is not None else None
        return result

    def getall(self, tablename, hints=[]):
        """"""
        if len(hints) == 0:
            keys = self.keys(tablename)
        else:
            id_hints = filter(lambda x:x[0] == "id", hints)
            non_id_hints = filter(lambda x:x[0] != "id", hints)
            sec_keys = map(lambda h: "sec_index:%s:%s:%s" % (tablename, h[0], h[1]), non_id_hints)
            keys = map(lambda x: "%s:id:%s" % (tablename, x[1]), id_hints)
            for sec_key in sec_keys:
                keys += self.redis_client.smembers(sec_key)
            # if len(sec_keys) > 0:
            #     # keys += filter(None, self.redis_client.smembers("sec_index:%s" % (tablename), sec_keys))
            #     keys += filter(None, self.redis_client.hmget("sec_index:%s" % (tablename), sec_keys))
            # # keys = filter(None, keys) + id_hints
        result = []
        keys = list(set(keys))
        if len(keys) > 0:
            str_result = self.redis_client.hmget(tablename, sorted(keys, key=lambda x: int(x.split(":")[-1])))
            # result = map(lambda x: json.loads(x), str_result)
            # toto = """{"virtual_interface": null, "pid": "0x10e70eb90", "updated_at": {"timezone": "None", "simplify_strategy": "datetime", "value": "Jun 01 2015 14:49:31"}, "session": null, "reserved": null, "allocated": null, "deleted_at": null, "id": 2001, "network": {"pid": "0x10e783758", "simplify_strategy": "novabase", "tablename": "networks", "id": 1, "novabase_classname": "Network"}, "virtual_interface_id": null, "floating_ips": [], "instance": null, "rome_version_number": 0, "rid": "68b90a35-086d-11e5-830e-b8e8563ae48c", "deleted": null, "leased": null, "host": null, "address": "172.1.0.1", "nova_classname": "fixed_ips", "instance_uuid": null, "network_id": 1, "created_at": {"timezone": "None", "simplify_strategy": "datetime", "value": "Jun 01 2015 14:49:31"}, "metadata_novabase_classname": "FixedIp"}"""
            python_toto = """{u'reserved': None, u'pid': u'%s', u'updated_at': {u'timezone': u'None', u'simplify_strategy': u'datetime', u'value': u'Jun 01 2015 14:49:31'}, u'session': None, u'virtual_interface': None, u'allocated': None, u'deleted_at': None, u'id': 2001, u'network': {u'tablename': u'networks', u'simplify_strategy': u'novabase', u'pid': u'0x10e783758', u'id': 1, u'novabase_classname': u'Network'}, u'virtual_interface_id': None, u'floating_ips': [], u'instance': None, u'metadata_novabase_classname': u'FixedIp', u'rid': u'68b90a35-086d-11e5-830e-b8e8563ae48c', u'deleted': None, u'leased': None, u'host': None, u'address': u'172.1.0.1', u'nova_classname': u'fixed_ips', u'instance_uuid': None, u'network_id': 1, u'created_at': {u'timezone': u'None', u'simplify_strategy': u'datetime', u'value': u'Jun 01 2015 14:49:31'}, u'rome_version_number': 0}"""
            # print(json.loads(toto))
            # for x in str_result:
            #     try:
            #         result += [eval(x)]
            #     except:
            #         print(x)
            #         pass
            #     pass
                # result.append(eval(python_toto % ("cuicui")))
            result = map(lambda x: eval(x), str_result)
            # result = map(lambda x: eval(python_toto), str_result)
            # result_str = "[%s]" %(",".join(str_result))
            # result = eval(result_str)
            # result = json.loads(result_str)
        return result

class RedisClusterDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        # startup_nodes = [{"host": "127.0.0.1", "port": "6379"}]
        startup_nodes = map(lambda x: {"host": x, "port": "%s" % (config.port())}, config.cluster_nodes())
        self.redis_client = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
        self.dlm = Redlock([{"host": "localhost", "port": 6379, "db": 0}, ], retry_count=10)
        # self.dlm = ClusterLock()

    def add_key(self, tablename, key):
        """"""
        pass

    def remove_key(self, tablename, key):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        self.redis_client.hdel(tablename, redis_key)
        pass

    def next_key(self, tablename):
        """"""
        next_key = self.redis_client.incr("nextkey:%s" % (tablename), 1)
        return next_key

    def keys(self, tablename):
        """"""
        """Check if the current table contains keys."""
        keys = self.redis_client.hkeys(tablename)
        return sorted(keys)

    def put(self, tablename, key, value, secondary_indexes=[]):
        """"""
        # lockname = "lock-%s" % (tablename)
        # my_lock = None
        # try_to_lock = True
        # while try_to_lock:
        #     my_lock = self.dlm.lock(lockname, 50)
        #     if my_lock is not False:
        #         try_to_lock = False
        #     else:
        #         time.sleep(0.020)
        # json_value = json.dumps(value)
        json_value = value
        fetched = self.redis_client.hset(tablename, "%s:id:%s" % (tablename, key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.redis_client.sadd("sec_index:%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
            # fetched = self.redis_client.hset("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
        result = value if fetched else None
        # self.dlm.unlock(my_lock)
        # self.dlm.unlock(lockname)
        return result

    def get(self, tablename, key, hint=None):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        if hint is not None:
            redis_keys = self.redis_client.smembers("sec_index:%s:%s:%s" % (tablename, hint[0], hint[1]))
            redis_key = redis_keys[0]
            # redis_key = self.redis_client.hget("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, hint[0], hint[1]))
        fetched = self.redis_client.hget(tablename, redis_key)
        # result = json.loads(fetched) if fetched is not None else None
        result = eval(fetched) if fetched is not None else None
        return result

    def getall(self, tablename, hints=[]):
        """"""
        if len(hints) == 0:
            keys = self.keys(tablename)
        else:
            id_hints = filter(lambda x:x[0] == "id", hints)
            non_id_hints = filter(lambda x:x[0] != "id", hints)
            sec_keys = map(lambda h: "sec_index:%s:%s:%s" % (tablename, h[0], h[1]), non_id_hints)
            keys = map(lambda x: "%s:id:%s" % (tablename, x[1]), id_hints)
            for sec_key in sec_keys:
                keys += self.redis_client.smembers(sec_key)
            # if len(sec_keys) > 0:
            #     # keys += filter(None, self.redis_client.smembers("sec_index:%s" % (tablename), sec_keys))
            #     keys += filter(None, self.redis_client.hmget("sec_index:%s" % (tablename), sec_keys))
            # # keys = filter(None, keys) + id_hints
        result = []
        keys = list(set(keys))
        if len(keys) > 0:
            str_result = self.redis_client.hmget(tablename, sorted(keys, key=lambda x:int(x.split(":")[-1])))
            # result = map(lambda x: json.loads(x), str_result)
            result = map(lambda x: eval(x), str_result)
        return result
