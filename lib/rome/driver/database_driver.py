from lib.rome.utils.MemoizationDecorator import memoization_decorator
from lib.rome.conf.Configuration import get_config
from lib.rome.utils.MemoizationDecorator import MemoizationDecorator

class DatabaseDriverInterface(object):

    def add_key(self, tablename, key):
        raise NotImplementedError

    def remove_key(self, tablename, key):
        raise NotImplementedError

    def next_key(self, tablename):
        raise NotImplementedError

    def keys(self, tablename):
        raise NotImplementedError

    def put(self, tablename, key, value, secondary_indexes=[]):
        raise NotImplementedError

    def get(self, tablename, key, hint=None):
        raise NotImplementedError

    def getall(self, tablename, hints=[]):
        raise NotImplementedError


driver = None

def build_driver():
    config = get_config()

    if config.backend() == "redis":
        import lib.rome.driver.redis.driver
        if config.redis_cluster_enabled():
            return lib.rome.driver.redis.driver.RedisClusterDriver()
        else:
            return lib.rome.driver.redis.driver.RedisDriver()
    if config.backend() == "cassandra":
        import lib.rome.driver.cassandra.driver
        return lib.rome.driver.cassandra.driver.CassandraDriver()
    else:
        import lib.rome.driver.riak.driver
        return lib.rome.driver.riak.driver.MapReduceRiakDriver()

def get_driver():
    # global driver
    # if driver is None:
    # driver = build_driver()
    return MemoizationDecorator(build_driver())
    # return build_driver()
