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
        next_key = self.redis_client.incr("nextkey-%s" % (tablename), 1)
        return next_key

    def keys(self, tablename):
        """"""
        """Check if the current table contains keys."""
        keys = self.redis_client.hkeys(tablename)
        # keys = self.redis_client.keys("%s-*" % (tablename))
        return keys

    def put(self, tablename, key, value):
        """"""
        json_value = json.dumps(value)
        fetched = self.redis_client.hset(tablename, "%s" % (key), json_value)
        # fetched = self.redis_client.set("%s-%s" % (tablename, key), json_value)
        result = value if fetched else None
        return result

    def get(self, tablename, key):
        """"""
        fetched = self.redis_client.hget(tablename, "%s" % (key))
        # fetched = self.redis_client.get("%s-%s" % (tablename, key))
        result = json.loads(fetched) if fetched is not None else None
        return result

    def getall(self, tablename):
        """"""
        keys = self.keys(tablename)
        if len(keys) > 0:
            str_result = self.redis_client.hmget(tablename, keys)
            # str_result = self.redis_client.mget(keys)
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
        next_key = self.redis_client.incr("nextkey-%s" % (tablename), 1)
        return next_key

    def keys(self, tablename):
        """"""
        """Check if the current table contains keys."""
        keys = self.redis_client.hkeys(tablename)
        return keys

    def put(self, tablename, key, value):
        """"""
        json_value = json.dumps(value)
        fetched = self.redis_client.hset(tablename, "%s" % (key), json_value)
        result = value if fetched else None
        return result

    def get(self, tablename, key):
        """"""
        fetched = self.redis_client.hget(tablename, "%s" % (key))
        result = json.loads(fetched) if fetched is not None else None
        return result

    def getall(self, tablename):
        """"""
        keys = self.keys(tablename)
        if len(keys) > 0:
            str_result = self.redis_client.hmget(tablename, keys)
            result = map(lambda x: json.loads(x), str_result)
            return result
        return []