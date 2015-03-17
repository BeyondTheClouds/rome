"""Desimplifier module.

This module contains functions, classes and mix-in that are used for the
desimplification of objects, before sending them to the services of nova.

"""

import datetime
import netaddr
import uuid
import pytz
import lib.rome.core.models as models
import lib.rome.core.lazy_reference as lazy_reference

CACHES = {}

def convert_to_camelcase(word):
    """Convert the given word into camelcase naming convention."""
    return ''.join(x.capitalize() or '_' for x in word.split('_'))


class JsonDeconverter(object):
    """Class that translate an object containing values taken from database
    into an object containing values understandable by services composing
    Nova."""

    def __init__(self, request_uuid=uuid.uuid1()):
        """Constructor"""

        self.request_uuid = (request_uuid if request_uuid is not None
                             else uuid.uuid1()
                             )
        if not CACHES.has_key(self.request_uuid):
            CACHES[self.request_uuid] = {}
        self.cache = CACHES[self.request_uuid]

    def is_dict_and_has_key(self, obj, key):
        """Check if the given object is a dict which contains the given key."""

        if isinstance(obj, dict):
            return obj.has_key(key)
        return False

    def get_key(self, obj):
        """Returns a unique key for the given object."""

        if self.is_dict_and_has_key(obj, "nova_classname"):
            table_name = obj["nova_classname"]
            key = obj["id"]
            return "%s_%s_%s" % (table_name, str(key), self.request_uuid)
        elif self.is_dict_and_has_key(obj, "novabase_classname"):
            table_name = obj["novabase_classname"]
            key = obj["id"]
            return "%s_%s_%s" % (table_name, str(key), self.request_uuid)
        else:
            return "%s_%s_%s" % (hex(id(obj)), hex(id(obj)), self.request_uuid)

    def novabase_desimplify(self, obj):
        """Desimplify a novabase object."""

        key = self.get_key(obj)
        if not self.cache.has_key(key):
            if obj.has_key("nova_classname"):
                tablename = obj["nova_classname"]
            elif obj.has_key("novabase_classname"):
                tablename = models.get_tablename_from_name(
                    obj["novabase_classname"]
                )
            else:
                tablename = models.get_tablename_from_name(
                    obj["metadata_novabase_classname"]
                )
            self.cache[key] = lazy_reference.LazyReference(
                tablename,
                obj["id"],
                desimplifier=self,
                request_uuid=self.request_uuid
            )
        return self.cache[key]


    def datetime_desimplify(self, value):
        """Desimplify a datetime object."""

        result = datetime.datetime.strptime(value["value"], '%b %d %Y %H:%M:%S')
        if value["timezone"] == "UTC":
            result = pytz.utc.localize(result)
        return result

    def ipnetwork_desimplify(self, value):
        """Desimplify an IPNetwork object."""

        return netaddr.IPNetwork(value["value"])

    def desimplify(self, obj):
        """Apply the best desimplification strategy on the given object."""

        result = obj

        is_dict = isinstance(obj, dict)
        is_list = isinstance(obj, list)

        if self.is_dict_and_has_key(obj, "simplify_strategy"):
            if obj['simplify_strategy'] == 'datetime':
                result = self.datetime_desimplify(obj)
            if obj['simplify_strategy'] == 'ipnetwork':
                result = self.ipnetwork_desimplify(obj)
            if obj['simplify_strategy'] == 'novabase':
                result = self.novabase_desimplify(obj)
        elif is_list:
            result = []
            for item in obj:
                result += [self.desimplify(item)]
        elif is_dict and obj.has_key("novabase_classname"):
            result = self.novabase_desimplify(obj)
        elif is_dict and obj.has_key("metadata_novabase_classname"):
            result = self.novabase_desimplify(obj)
        elif is_dict:
            result = {}
            for item in obj:
                result[item] = self.desimplify(obj[item])
        return result
