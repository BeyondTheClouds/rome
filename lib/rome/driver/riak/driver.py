import lib.rome.driver.database_driver
import riak
from riak.datatypes import Counter

class RiakDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        self.riak_client = riak.RiakClient(pb_port=8087, protocol='pbc')

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

    def put(self, tablename, key, value):
        """"""
        bucket = self.riak_client.bucket(tablename)
        fetched = bucket.new("%s" % (key), data=value)
        fetched.store()
        return fetched

    def get(self, tablename, key):
        """"""
        bucket = self.riak_client.bucket(tablename)
        return bucket.get("%s" % (key)).data

    def getall(self, tablename):
        """"""
        bucket = self.riak_client.bucket(tablename)
        keys = map(lambda x:str(x), self.keys(tablename))
        result = map(lambda x:x.data, bucket.multiget(keys))
        return result