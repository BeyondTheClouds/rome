import lib.rome.driver.database_driver
import riak

class RiakDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        self.riak_client = riak.RiakClient(pb_port=8087, protocol='pbc')

    def add_key(self, tablename, key):
        """"""
        keys = self.keys(tablename)
        if not key in keys:
            keys.append(key)
            bucket = self.riak_client.bucket("key_index")
            fetched = bucket.get(tablename)
            fetched.data = keys
            fetched.store()
        return keys

    def remove_key(self, tablename, key):
        """"""
        keys = self.keys(tablename)
        if key in keys:
            keys.remove(key)
            bucket = self.riak_client.bucket("key_index")
            fetched = bucket.get(tablename)
            fetched.data = keys
            fetched.store()
        return keys

    def next_key(self, tablename):
        """"""
        keys = self.keys(tablename)
        current_key = 1
        while current_key in keys:
            current_key += 1
        return current_key

    def keys(self, tablename):
        """"""
        """Check if the current table contains keys."""
        bucket = self.riak_client.bucket("key_index")
        fetched = bucket.get(tablename)
        """If no keys: initialize an empty list of keys."""
        if fetched.data == None:
            empty_keys = bucket.new(tablename, data=[])
            empty_keys.store()
        fetched = bucket.get(tablename)
        keys = fetched.data if fetched.data != None else []
        return keys

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