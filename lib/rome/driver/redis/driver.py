import lib.rome.driver.database_driver
import redis
import json


class RedisDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

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
        keys = self.redis_client.keys("%s-*" % (tablename))
        return keys

    def put(self, tablename, key, value):
        """"""
        json_value = json.dumps(value)
        fetched = self.redis_client.set("%s-%s" % (tablename, key), json_value)
        result = json.loads(fetched)
        return result

    def get(self, tablename, key):
        """"""
        fetched = self.redis_client.get("%s-%s" % (tablename, key))
        result = json.loads(fetched) if fetched is not None else None
        return result

    def getall(self, tablename):
        """"""
        keys = self.keys(tablename)
        if len(keys) > 0:
            str_result = self.redis_client.mget(keys)
            result = map(lambda x: json.loads(x), str_result)
            return result
        return []