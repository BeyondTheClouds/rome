
class DatabaseDriverInterface(object):

    def add_key(self, tablename, key):
        raise NotImplementedError

    def remove_key(self, tablename, key):
        raise NotImplementedError

    def next_key(self, tablename):
        raise NotImplementedError

    def keys(self, tablename):
        raise NotImplementedError

    def put(self, tablename, key, value):
        raise NotImplementedError

    def get(self, tablename, key):
        raise NotImplementedError

    def getall(self, tablename):
        raise NotImplementedError


driver = None

def get_driver():
    global driver
    if driver is None:
        # import lib.rome.driver.riak.driver
        # driver = lib.rome.driver.riak.driver.RiakDriver()
        # driver = lib.rome.driver.riak.driver.MapReduceRiakDriver()

        import lib.rome.driver.redis.driver
        driver = lib.rome.driver.redis.driver.RedisDriver()
    return driver
