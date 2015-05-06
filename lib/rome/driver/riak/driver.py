import lib.rome.driver.database_driver
import riak
from riak.datatypes import Counter
from multiprocessing import Pool
import json
from lib.rome.conf.Configuration import get_config

class RiakDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        self.riak_client = riak.RiakClient(pb_port=config.port(), protocol='pbc')

    def add_key(self, tablename, key):
        """"""
        pass

    def remove_key(self, tablename, key):
        """"""
        bucket = self.riak_client.bucket(tablename)
        return bucket.delete("%s" % (key))

    def next_key(self, tablename):
        """"""
        bucket = self.riak_client.bucket_type('counters').bucket(tablename)
        counter = Counter(bucket, "next_key")
        counter.increment()
        counter.store()
        counter.reload()
        return counter.value

    def keys(self, tablename):
        """"""
        """Check if the current table contains keys."""
        bucket = self.riak_client.bucket(tablename)
        return bucket.get_keys()

    def put(self, tablename, key, value, secondary_indexes=[]):
        """"""
        bucket = self.riak_client.bucket(tablename)
        fetched = bucket.new("%s" % (key), data=value)
        fetched.store()
        return fetched

    def get(self, tablename, key, hint=None):
        """"""
        bucket = self.riak_client.bucket(tablename)
        return bucket.get("%s" % (key)).data

    def getall(self, tablename, hints=[]):
        """"""
        keys = map(lambda x:str(x), self.keys(tablename))
        bucket = self.riak_client.bucket(tablename)
        result = map(lambda x:x.data, bucket.multiget(keys))
        return result

class MapReduceRiakDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        self.riak_client = riak.RiakClient(pb_port=config.port(), protocol='pbc')

    def add_key(self, tablename, key):
        """"""
        pass

    def remove_key(self, tablename, key):
        """"""
        bucket = self.riak_client.bucket(tablename)
        return bucket.delete("%s-%s" % (tablename, key))

    def next_key(self, tablename):
        """"""
        bucket = self.riak_client.bucket_type('counters').bucket(tablename)
        counter = Counter(bucket, "next_key")
        counter.increment()
        counter.store()
        counter.reload()
        return counter.value

    def keys(self, tablename):
        """"""
        """Check if the current table contains keys."""
        mapReduce = riak.RiakMapReduce(self.riak_client)
        mapReduce.add(tablename)
        mapReduce.add_key_filter("starts_with", "%s-" % (tablename))
        mapReduce.map("function (v, keydata) { return [v.key]; }")
        results = mapReduce.run()
        return results if results is not None else []

    def put(self, tablename, key, value, secondary_indexes=[]):
        """"""
        bucket = self.riak_client.bucket(tablename)
        fetched = bucket.new("%s-%s" % (tablename, key), data=value)
        fetched.store()
        return fetched

    def get(self, tablename, key, hint=None):
        """"""
        bucket = self.riak_client.bucket(tablename)
        return bucket.get("%s-%s" % (tablename, key)).data

    def getall(self, tablename, hints=[]):
        """"""
        keys = map(lambda x:str(x), self.keys(tablename))
        if len(keys) > 0:
            mapReduce = riak.RiakMapReduce(self.riak_client)
            mapReduce.add(tablename, keys)
            mapReduce.map("function(v) {return [v.values[0].data]}")
            result = map(lambda x: json.loads(x), mapReduce.run())
        else:
            result = []
        return result