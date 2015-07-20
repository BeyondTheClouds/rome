import redis

import lib.rome.driver.database_driver
from cassandra.cluster import Cluster
from lib.rome.conf.Configuration import get_config

from lib.rome.driver.redis.lock import ClusterLock as ClusterLock
# from cassandra.query import dict_factory
# from cassandra.encoder import Encoder

from lib.rome.core.dataformat.json import Decoder as JsonDecoder
from lib.rome.core.dataformat.string import Decoder as StringDecoder
from lib.rome.core.dataformat.string import Encoder as StringEncoder


def decoded_dict_factory(colnames, rows):
    """
    Returns each row as a dict; each value of the dict has been processed by StringDecoder.

    Example::

        >>> from cassandra.query import dict_factory
        >>> session = cluster.connect('mykeyspace')
        >>> session.row_factory = dict_factory
        >>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
        >>> print rows[0]
        {u'age': 42, u'name': u'Bob'}

    .. versionchanged:: 2.0.0
        moved from ``cassandra.decoder`` to ``cassandra.query``
    """
    # json_decoder = JsonDecoder()
    string_decoder = StringDecoder()

    decoded_rows = map(lambda x: string_decoder.desimplify(x), rows)
    # return [dict(zip(colnames, row)) for row in rows]
    return [dict(zip(colnames, row)) for row in decoded_rows]

class CassandraDriver(lib.rome.driver.database_driver.DatabaseDriverInterface):

    def __init__(self):
        config = get_config()
        self.redis_client = redis.StrictRedis(host=config.host(), port=config.port(), db=0)
        self.cluster = Cluster()
        self.session = self.cluster.connect('mykeyspace')
        self.session.row_factory = decoded_dict_factory
        self._tables = {}
        # self.dlm = ClusterLock()

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
        """Check if the current table contains keys."""
        keys = self.redis_client.hkeys(tablename)
        return sorted(keys)

    def _extract_fields(self, tablename):
        from lib.rome.core.models import get_model_class_from_name, get_model_classname_from_tablename
        modelclass_name = get_model_classname_from_tablename(tablename)
        klass = get_model_class_from_name(modelclass_name)
        fields = []
        try:
            fields = map(lambda x: "%s" % (x.key), klass()._sa_instance_state.attrs)
        except:
            fields = map(lambda x: "%s" % (x), klass._sa_class_manager)
        fields += ["pid", "metadata_novabase_classname", "rid", "nova_classname", "rome_version_number"]
        fields = sorted(list(set(fields)))
        print("fields@%s => %s" % (tablename, fields))
        return fields

    def _correct_badname(self, name):
        if name[0] == "_":
            return "cs_%s" % (name[1:])
        else:
            return name

    def _table_exist(self, tablename):

        if self._tables.has_key(tablename):
            return True

        cql_request = "select * from %s" % (tablename)
        try:
            self.session.execute(cql_request)
        except:
            return False
        return True

    def process_column(column_name, klass):
        column_name_and_type = ""
        if hasattr(getattr(klass, column_name, None), "impl") and getattr(klass, column_name).impl.collection:
            column_name_and_type = "%s VARCHAR" % (column_name)
        elif hasattr(klass, column_name):
            column_type = "%s" % (getattr(klass, column_name).expression.type)
            if column_type == "DATETIME" or "VARCHAR" in column_type:
                column_name_and_type = "%s varchar" % (column_name)
            elif column_type == "INTEGER":
                column_name_and_type = "%s int" % (column_name)
            else:
                column_name_and_type = "%s %s" % (column_name, column_type)
        return column_name_and_type

    def _table_create(self, tablename):
        from lib.rome.core.models import get_model_class_from_name, get_model_classname_from_tablename
        modelclass_name = get_model_classname_from_tablename(tablename)
        klass = get_model_class_from_name(modelclass_name)

        fields = self._extract_fields(tablename)
        corrected_columns = map(lambda x: self._correct_badname(x), fields)
        corrected_columns = filter(lambda x: x!="rome_version_number", corrected_columns)
        columns_name_str = ", ".join(map(lambda x: "%s varchar" % (x), corrected_columns))
        # columns_name_str = ", ".join(map(lambda x: process_column(x, klass), corrected_columns))

        columns_name_str += ", rome_version_number int"
        cql_request = "create table %s (%s, PRIMARY KEY(id))" % (tablename, columns_name_str)
        print(cql_request)
        try:
            result = self.session.execute(cql_request)
            self._tables[tablename] = tablename
        except:
            pass
        return result

    def put(self, tablename, key, value, secondary_indexes=[]):
        """"""
        if not self._table_exist(tablename):
            self._table_create(tablename)
        filtered_value = dict((k,v) for k,v in value.iteritems() if v is not None and k != "rome_version_number")
        columns = filtered_value.keys()
        corrected_columns = list(set(columns))

        string_encoder = StringEncoder()

        columns_name_str = ", ".join(map(lambda x: "%s" % (x), corrected_columns))
        # columns_value_str = ", ".join(map(lambda x: ("%s" % ((("%s") % (value[x])).replace("'", "\""))), corrected_columns))
        encoded_values = map(lambda x: "\'%s\'" % (("%s" % (string_encoder.simplify(value[x]))).replace("'", "\"")), corrected_columns)
        columns_value_str = ", ".join(encoded_values)

        columns_name_str += ", rome_version_number"
        columns_value_str += ", %s" % (value["rome_version_number"])

        cql_request = "insert into %s (%s) values (%s)" % (tablename, columns_name_str, columns_value_str)
        print(cql_request)
        result = self.session.execute(cql_request)

        return result

    def get(self, tablename, key, hint=None):
        """"""
        if not self._table_exist(tablename):
            self._table_create(tablename)
        cql_request = """select * from %s where id='%s'""" % (tablename, key)
        result = self.session.execute(cql_request)
        if len(result) > 0:
            return result[0]
        else:
            return None

    def getall(self, tablename, hints=[]):
        """"""
        if not self._table_exist(tablename):
            self._table_create(tablename)
        cql_request = """select * from %s""" % (tablename)
        result = self.session.execute(cql_request)
        return result
