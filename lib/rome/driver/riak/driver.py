import lib.rome.driver.database_driver
import riak
from riak.datatypes import Counter
from functools import partial

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
        keys = map(lambda x:str(x), self.keys(tablename))
        bucket = self.riak_client.bucket(tablename)
        result = map(lambda x:x.data, bucket.multiget(keys))
        return result

class ParallelMultigetdRiakDriver(RiakDriver):
    
    def getall(self, tablename):
        """"""
        keys = map(lambda x:str(x), self.keys(tablename))
        if len(keys) > 100:
            from multiprocessing import Pool

            multiget_request_size = 10
            partitioned_keys = [keys[i: i+multiget_request_size] for i in xrange(0, len(keys), multiget_request_size)]
            pool_size = len(partitioned_keys)
            process_pool = Pool(pool_size)
            p_results = list(process_pool.map(create_multiget(tablename), partitioned_keys))
            result = [item for sublist in p_results for item in sublist]
            process_pool.shutdown(wait=False)
        else:
            result = super(ParallelMultigetdRiakDriver, self).getall()
        return result

class ParallelMultigetdProcessPoolExecutorRiakDriver(RiakDriver):

    def getall(self, tablename):
        """"""
        keys = map(lambda x:str(x), self.keys(tablename))
        if len(keys) > 100:
            from concurrent.futures import ProcessPoolExecutor

            multiget_request_size = 10
            partitioned_keys = [keys[i: i+multiget_request_size] for i in xrange(0, len(keys), multiget_request_size)]
            pool_size = len(partitioned_keys)
            process_pool = ProcessPoolExecutor(max_workers=pool_size)
            p_results = list(process_pool.map(create_multiget(tablename), partitioned_keys))
            result = [item for sublist in p_results for item in sublist]
            process_pool.shutdown(wait=False)
        else:
            result = super(ParallelMultigetdRiakDriver, self).getall()
        return result



def multiget(keys, tablename=None):
    riak_client = riak.RiakClient(pb_port=8087, protocol='pbc')
    bucket = riak_client.bucket(tablename)
    return [x.data for x in bucket.multiget(keys)]

def create_multiget(tablename):
    return partial(multiget, tablename=tablename)