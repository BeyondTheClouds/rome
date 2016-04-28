import redis

import lib.rome.driver.database_driver

import rediscluster
from lib.rome.conf.Configuration import get_config
from redlock import Redlock as Redlock

import ujson
from Queue import Queue


# import eventlet
# eventlet.monkey_patch(thread=True)
import multiprocessing
# import thread
# reload(thread)
import os

PARALLEL_STRUCTURES = {}

def easy_parallelize_multiprocessing(f, sequence):
    if not "eval_pool" in PARALLEL_STRUCTURES:
        from multiprocessing import Pool
        import multiprocessing
        NCORES = multiprocessing.cpu_count()
        eval_pool = Pool(processes=NCORES)
        PARALLEL_STRUCTURES["eval_pool"] = eval_pool
    eval_pool = PARALLEL_STRUCTURES["eval_pool"]
    result = eval_pool.map(f, sequence)
    cleaned = [x for x in result if not x is None]
    return cleaned


def easy_parallelize_sequence(f, sequence):
    if sequence is None:
        return []
    return map(f, sequence)


def easy_parallelize_gevent(f, sequence):
    if not "gevent_pool" in PARALLEL_STRUCTURES:
        from gevent.threadpool import ThreadPool
        pool = ThreadPool(30000)
        PARALLEL_STRUCTURES["gevent_pool"] = pool
    pool = PARALLEL_STRUCTURES["gevent_pool"]
    result = pool.map(f, sequence)
    return result


def easy_parallelize_eventlet(f, sequence):
    import eventlet
    green_pool_size = len(sequence) + 1
    pool = eventlet.GreenPool(size=green_pool_size)
    q = Queue()
    def wrapp_f(f, e, q):
        # q.put(f(e))
        f(e)
    result = []
    for e in sequence:
        pool.spawn_n(f, e)
    pool.waitall()
    return result


# def easy_parallelize(f, sequence):
#     # try:
#     #     result = easy_parallelize_multiprocessing(f, sequence)
#     #     # easy_parallelize_eventlet(f, sequence)
#     #     print("using multiprocessing")
#     #     return result
#     # except:
#     return easy_parallelize_sequence(f, sequence)
#     # return easy_parallelize_gevent(f, sequence)
#     # return easy_parallelize_eventlet(f, sequence)


easy_parallelize = easy_parallelize_sequence
# easy_parallelize = lambda x, y: []

def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def flatten(container):
    for i in container:
        if isinstance(i, list) or isinstance(i, tuple):
            for j in flatten(i):
                yield j
        else:
            yield i

def convert_unicode_dict_to_utf8(d):
    if d is None:
        return None
    result = {}
    for key in d:
        v = d[key]
        if type(v) is dict:
            v = convert_unicode_dict_to_utf8(d[key])
        if type(v) is unicode:
            v = v.encode('utf-8')
        result[str(key)] = v
    return result


class RedisDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        global eval_pool
        config = get_config()
        self.redis_client = redis.StrictRedis(host=config.host(), port=config.port(), db=0)
        self.dlm = Redlock([{"host": "localhost", "port": 6379, "db": 0}, ], retry_count=10)

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

        """ Dump python object to JSON field. """
        json_value = ujson.dumps(value)
        fetched = self.redis_client.hset(tablename, "%s:id:%s" % (tablename, key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.redis_client.sadd("sec_index:%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
        result = value if fetched else None
        result = convert_unicode_dict_to_utf8(result)
        return result

    def get(self, tablename, key, hint=None):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        if hint is not None:
            redis_keys = self.redis_client.smembers("sec_index:%s:%s:%s" % (tablename, hint[0], hint[1]))
            redis_key = redis_keys[0]
        fetched = self.redis_client.hget(tablename, redis_key)

        """ Parse result from JSON to python dict. """
        result = ujson.loads(fetched) if fetched is not None else None
        return result

    def _resolve_keys(self, tablename, keys):
        result = []
        if len(keys) > 0:
            keys = filter(lambda x: x != "None" and x != None, keys)
            str_result = self.redis_client.hmget(tablename, sorted(keys, key=lambda x: x.split(":")[-1]))

            """ When looking-up for a deleted object, redis's driver return None, which should be filtered."""
            str_result = filter(lambda x: x is not None, str_result)

            """ Transform the list of JSON string into a single string (boost performances). """
            str_result = "[%s]" % (",".join(str_result))

            """ Parse result from JSON to python dict. """
            result = ujson.loads(str_result)
            result = map(lambda x: convert_unicode_dict_to_utf8(x), result)
            result = filter(lambda x: x!= None, result)
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
        keys = list(set(keys))
        return self._resolve_keys(tablename, keys)


class RedisClusterDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        startup_nodes = map(lambda x: {"host": x, "port": "%s" % (config.port())}, config.cluster_nodes())
        self.redis_client = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
        self.dlm = Redlock([{"host": "localhost", "port": 6379, "db": 0}, ], retry_count=10)

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

        """ Dump python object to JSON field. """
        json_value = ujson.dumps(value)
        fetched = self.redis_client.hset(tablename, "%s:id:%s" % (tablename, key), json_value)
        for secondary_index in secondary_indexes:
            secondary_value = value[secondary_index]
            fetched = self.redis_client.sadd("sec_index:%s:%s:%s" % (tablename, secondary_index, secondary_value), "%s:id:%s" % (tablename, key))
        result = value if fetched else None
        result = convert_unicode_dict_to_utf8(result)
        return result

    def get(self, tablename, key, hint=None):
        """"""
        redis_key = "%s:id:%s" % (tablename, key)
        if hint is not None:
            redis_keys = self.redis_client.smembers("sec_index:%s:%s:%s" % (tablename, hint[0], hint[1]))
            redis_key = redis_keys[0]
        fetched = self.redis_client.hget(tablename, redis_key)

        """ Parse result from JSON to python dict. """
        result = ujson.loads(fetched) if fetched is not None else None
        return result

    def _resolve_keys(self, tablename, keys):
        result = []
        if len(keys) > 0:
            keys = filter(lambda x: x != "None" and x != None, keys)
            str_result = self.redis_client.hmget(tablename, sorted(keys, key=lambda x: x.split(":")[-1]))

            """ When looking-up for a deleted object, redis's driver return None, which should be filtered."""
            str_result = filter(lambda x: x is not None, str_result)

            """ Transform the list of JSON string into a single string (boost performances). """
            str_result = "[%s]" % (",".join(str_result))

            """ Parse result from JSON to python dict. """
            result = ujson.loads(str_result)
            result = map(lambda x: convert_unicode_dict_to_utf8(x), result)
            result = filter(lambda x: x!= None, result)
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
        keys = list(set(keys))
        return self._resolve_keys(tablename, keys)