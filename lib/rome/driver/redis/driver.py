import lib.rome.driver.database_driver
import redis
import json
import rediscluster
from lib.rome.conf.Configuration import get_config

class RedisDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        self.redis_client = redis.StrictRedis(host=config.host(), port=config.port(), db=0)

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
        return keys

    def put(self, tablename, key, value, secondary_indexes=[]):
        """"""
        json_value = json.dumps(value)
        fetched = self.redis_client.hset(tablename, "%s:id:%s" % (tablename, key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.redis_client.hset("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
        result = value if fetched else None
        return result

    def get(self, tablename, key, hint=None):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        if hint is not None:
            redis_key = self.redis_client.hget("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, hint[0], hint[1]))
        fetched = self.redis_client.hget(tablename, redis_key)
        result = json.loads(fetched) if fetched is not None else None
        return result

    def getall(self, tablename, hints=[]):
        """"""
        if len(hints) == 0:
            keys = self.keys(tablename)
        else:
            sec_keys = map(lambda h: "%s:%s:%s" % (tablename, h[0], h[1]), hints)
            keys = self.redis_client.hmget("sec_index:%s" % (tablename), sec_keys)
        if len(keys) > 0:
            str_result = self.redis_client.hmget(tablename, keys)
            result = map(lambda x: json.loads(x), str_result)
            return result
        return []

class RedisClusterDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        # startup_nodes = [{"host": "127.0.0.1", "port": "6379"}]
        startup_nodes = map(lambda x: {"host": x, "port": "%s" % (config.port())}, config.cluster_nodes())
        self.redis_client = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

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
        return keys

    def put(self, tablename, key, value, secondary_indexes=[]):
        """"""
        json_value = json.dumps(value)
        fetched = self.redis_client.hset(tablename, "%s:id:%s" % (tablename, key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.redis_client.hset("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
        result = value if fetched else None
        return result

    def get(self, tablename, key, hint=None):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        if hint is not None:
            redis_key = self.redis_client.hget("sec_index:%s" % (tablename), "%s:%s:%s" % (tablename, hint[0], hint[1]))
        fetched = self.redis_client.hget(tablename, redis_key)
        result = json.loads(fetched) if fetched is not None else None
        return result

    def getall(self, tablename, hints=[]):
        """"""
        if len(hints) == 0:
            keys = self.keys(tablename)
        else:
            sec_keys = map(lambda h: "%s:%s:%s" % (tablename, h[0], h[1]), hints)
            keys = self.redis_client.hmget("sec_index:%s" % (tablename), sec_keys)
        if len(keys) > 0:
            str_result = self.redis_client.hmget(tablename, keys)
            result = map(lambda x: json.loads(x), str_result)
            return result
        return []