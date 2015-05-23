import lib.rome.driver.database_driver
import redis
import json
import rediscluster
from lib.rome.conf.Configuration import get_config
# from redlock import RedLock as RedLock
# import redis_lock
from redlock import Redlock as Redlock

class RedisDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        self.redis_client = redis.StrictRedis(host=config.host(), port=config.port(), db=0)
        self.dlm = Redlock([{"host": "localhost", "port": 6379, "db": 0}, ], retry_count=10)

    def add_key(self, tablename, key):
        """"""
        pass

    def remove_key(self, tablename, key):
        """"""
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
        lockname = "lock-%s" % (tablename)
        my_lock = self.dlm.lock(lockname,1000)
        json_value = json.dumps(value)
        fetched = self.redis_client.hset(tablename, "%s:id:%s" % (tablename, key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.redis_client.sadd("sec_index:%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
            # fetched = self.redis_client.hset("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
        result = value if fetched else None
        self.dlm.unlock(my_lock)
        return result

    def get(self, tablename, key, hint=None):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        if hint is not None:
            redis_keys = self.redis_client.smembers("sec_index:%s:%s:%s" % (tablename, hint[0], hint[1]))
            redis_key = redis_keys[0]
            # redis_key = self.redis_client.hget("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, hint[0], hint[1]))
        fetched = self.redis_client.hget(tablename, redis_key)
        result = json.loads(fetched) if fetched is not None else None
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
            result = map(lambda x: json.loads(x), str_result)
        return result

class RedisClusterDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        # startup_nodes = [{"host": "127.0.0.1", "port": "6379"}]
        startup_nodes = map(lambda x: {"host": x, "port": "%s" % (config.port())}, config.cluster_nodes())
        self.redis_client = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
        self.dlm = Redlock([{"host": "localhost", "port": 6379, "db": 0}, ], retry_count=10)

    def add_key(self, tablename, key):
        """"""
        pass

    def remove_key(self, tablename, key):
        """"""
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
        lockname = "lock-%s" % (tablename)
        my_lock = self.dlm.lock(lockname,1000)
        json_value = json.dumps(value)
        fetched = self.redis_client.hset(tablename, "%s:id:%s" % (tablename, key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.redis_client.sadd("sec_index:%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
            # fetched = self.redis_client.hset("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
        result = value if fetched else None
        self.dlm.unlock(my_lock)
        return result

    def get(self, tablename, key, hint=None):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        if hint is not None:
            redis_keys = self.redis_client.smembers("sec_index:%s:%s:%s" % (tablename, hint[0], hint[1]))
            redis_key = redis_keys[0]
            # redis_key = self.redis_client.hget("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, hint[0], hint[1]))
        fetched = self.redis_client.hget(tablename, redis_key)
        result = json.loads(fetched) if fetched is not None else None
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
            result = map(lambda x: json.loads(x), str_result)
        return result