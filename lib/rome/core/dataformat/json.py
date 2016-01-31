"""JSON module.

This module contains functions, classes and mix-in that are used for the
simplification of objects, before storing them into the JSON database.

"""

import lib.rome.core.utils as utils
import uuid
from lib.rome.utils.LimitedSizeDictionnary import LimitedSizeDictionnary
import datetime
import netaddr
import uuid
import pytz
import lib.rome.core.models as models
import lib.rome.core.lazy as lazy_reference
from lib.rome.utils.LimitedSizeDictionnary import LimitedSizeDictionnary

CACHES = LimitedSizeDictionnary(size_limit=20)
SIMPLE_CACHES = LimitedSizeDictionnary(size_limit=20)
COMPLEX_CACHES = LimitedSizeDictionnary(size_limit=20)
TARGET_CACHES = LimitedSizeDictionnary(size_limit=20)

from lib.rome.core.utils import DATE_FORMAT

def extract_adress(obj):
    """Extract an indentifier for the given object: if the object contains an
    id, it returns the id, otherwise it returns the memory address of the
    given object."""

    result = hex(id(obj))
    try:
        if utils.is_novabase(obj):
            result = str(obj).split("at ")[1].split(">")[0]
    except:
        pass
    return result


class Encoder(object):
    """A class that is in charge of converting python objects (basic types,
    dictionnaries, novabase objects, ...) to a representation that can
    be stored in database."""

    def __init__(self, request_uuid=uuid.uuid1()):
        self.request_uuid = (request_uuid if request_uuid is not None
                             else uuid.uuid1()
                             )
        if not SIMPLE_CACHES.has_key(self.request_uuid):
            SIMPLE_CACHES[self.request_uuid] = {}
        if not COMPLEX_CACHES.has_key(self.request_uuid):
            COMPLEX_CACHES[self.request_uuid] = {}
        if not TARGET_CACHES.has_key(self.request_uuid):
            TARGET_CACHES[self.request_uuid] = {}

        self.simple_cache = SIMPLE_CACHES[self.request_uuid]
        self.complex_cache = COMPLEX_CACHES[self.request_uuid]
        self.target_cache = TARGET_CACHES[self.request_uuid]

        self.reset()

    def get_cache_key(self, obj):
        """Compute an "unique" key for the given object: this key is used to
        when caching objects."""

        classname = obj.__class__.__name__
        if classname == "LazyReference" or classname == "LazyValue":
            return obj.get_key()

        if hasattr(obj, "id") and getattr(obj, "id") is not None:
            key = "%s_%s" % (classname, obj.id)
        else:
            key = "%s_x%s" % (classname, extract_adress(obj))
        return key

    def already_processed(self, obj):
        """Check if the given object has been processed, according to its
        unique key."""
        if hasattr(obj, "is_relationship_list"):
            return True
        key = self.get_cache_key(obj)
        return self.simple_cache.has_key(key)

    def datetime_simplify(self, datetime_ref):
        """Simplify a datetime object."""

        return {
            "simplify_strategy": "datetime",
            "value": datetime_ref.strftime(DATE_FORMAT),
            "timezone": str(datetime_ref.tzinfo)
        }

    def ipnetwork_simplify(self, ipnetwork):
        """Simplify an IP address object."""

        return {
            "simplify_strategy": "ipnetwork",
            "value": str(ipnetwork)
        }

    def process_field(self, field_value):
        """Inner function that processes a value."""

        if utils.is_novabase(field_value):
            if not self.already_processed(field_value):
                self.process_object(field_value, False)
            key = self.get_cache_key(field_value)
            result = self.simple_cache[key]
        else:
            result = self.process_object(field_value, False)
        return result

    def extract_complex_object(self, obj):
        """Extract an object where each attribute has been simplified."""

        fields_iterator = None
        if hasattr(obj, "_sa_class_manager"):
            fields_iterator = obj._sa_class_manager
        elif hasattr(obj, "__dict__"):
            fields_iterator = obj.__dict__
        elif obj.__class__.__name__ == "dict":
            fields_iterator = obj

        complex_object = {}
        relationships_fields = map(lambda x: x.local_object_field, obj.get_relationships())
        if fields_iterator is not None:
            for field in fields_iterator:
                field_value = getattr(obj, field)
                if field in relationships_fields:
                    self.process_object(field_value)
                    continue
                if utils.is_novabase(field_value):
                    complex_object[field] = self.process_field(field_value)
                elif isinstance(field_value, list): #or hasattr(field_value, "is_relationship_list"):
                    field_list = []
                    for item in field_value:
                        field_list += [self.process_field(item)]
                    complex_object[field] = field_list
                else:
                    complex_object[field] = self.process_field(field_value)
        return complex_object

    def novabase_simplify(self, obj, skip_complex_processing=False):
        """Simplify a NovaBase object."""

        if not self.already_processed(obj):

            # obj.update_foreign_keys()
            key = self.get_cache_key(obj)

            if self.simple_cache.has_key(key):
                simplified_object = self.simple_cache[key]
            else:
                novabase_classname = str(obj.__class__.__name__)
                if novabase_classname == "LazyReference" or novabase_classname == "LazyValue":
                    novabase_classname = obj.resolve_model_name()
                if isinstance(obj, dict) and "novabase_classname" in obj:
                    novabase_classname = obj["novabase_classname"]
                tmp = {
                    "simplify_strategy": "novabase",
                    "tablename": obj.__tablename__,
                    "novabase_classname": novabase_classname,
                    "id": obj.id,
                    "_pid": extract_adress(obj)
                }
                if hasattr(tmp, "user_id"):
                    tmp = utils.merge_dicts(obj, {"user_id": obj.user_id})
                if hasattr(tmp, "project_id"):
                    tmp = utils.merge_dicts(tmp, {"project_id": obj.project_id})
                if not key in self.simple_cache:
                    self.simple_cache[key] = tmp
                    self.target_cache[key] = obj

                simplified_object = tmp

            if skip_complex_processing:
                return simplified_object

            key = self.get_cache_key(obj)
            if not key in self.simple_cache:
                self.simple_cache[key] = simplified_object
                self.target_cache[key] = obj

            complex_object = self.extract_complex_object(obj)

            metadata_class_name = novabase_classname
            complex_object["_metadata_novabase_classname"] = metadata_class_name
            complex_object["_pid"] = extract_adress(obj)
            complex_object["_rid"] = str(self.request_uuid)

            if not key in self.complex_cache:
                self.complex_cache[key] = complex_object
        else:
            key = self.get_cache_key(obj)
            simplified_object = self.simple_cache[key]
        return simplified_object

    def object_simplify(self, obj):
        """Convert this object to dictionnary that contains simplified values:
        every value is simplified according to the appropriate strategy."""

        result = obj
        do_deep_simplification = False
        is_basic_type = False

        try:
            if hasattr(obj, "__dict__") or obj.__class__.__name__ == "dict":
                do_deep_simplification = True
        except:
            is_basic_type = True

        if do_deep_simplification and not is_basic_type:

            novabase_classname = str(obj.__class__.__name__)
            if novabase_classname == "LazyReference" or novabase_classname == "LazyValue":
                novabase_classname = obj.resolve_model_name()
            if isinstance(obj, dict) and "novabase_classname" in obj:
                novabase_classname = obj["novabase_classname"]

            # Initialize fields to iterate
            if hasattr(obj, "reload_default_values"):
                obj.reload_default_values()

            result = self.extract_complex_object(obj)

            if utils.is_novabase(obj):
                key = self.get_cache_key(obj)
                if not key in self.complex_cache:
                    self.complex_cache[key] = result
                    self.simple_cache[key] = self.novabase_simplify(obj, True)
                    self.target_cache[key] = obj

                    metadata_class_name = novabase_classname
                    metadata_dict = {
                        "_metadata_novabase_classname": metadata_class_name,
                        "_pid": extract_adress(obj),
                        "_rid": str(self.request_uuid)
                    }
                    self.complex_cache[key] = utils.merge_dicts(
                        self.complex_cache[key],
                        metadata_dict
                    )
                    self.simple_cache[key] = utils.merge_dicts(
                        self.simple_cache[key],
                        metadata_dict
                    )
                    result = self.complex_cache[key]

        return result

    def process_object(self, obj, skip_reccursive_call=True):
        """Apply the best simplification strategy to the given object."""
        from lib.rome.core.lazy import LazyValue
        should_skip = self.already_processed(obj) or skip_reccursive_call

        if utils.is_novabase(obj):
            if should_skip:
                result = self.novabase_simplify(obj)
            else:
                key = self.get_cache_key(obj)
                self.novabase_simplify(obj)
                result = self.complex_cache[key]
        elif obj.__class__.__name__ == "datetime":
            result = self.datetime_simplify(obj)
        elif obj.__class__.__name__ == "IPNetwork":
            result = self.ipnetwork_simplify(obj)
        else:
            result = obj

        return result

    def reset(self):
        """Reset the caches of the current instance of Simplifier."""

        self.simple_cache = {}
        self.complex_cache = {}
        self.target_cache = {}

    def simplify(self, obj):
        """Simplify the given object."""

        result = self.process_object(obj, False)
        return result


def convert_to_camelcase(word):
    """Convert the given word into camelcase naming convention."""
    return ''.join(x.capitalize() or '_' for x in word.split('_'))


def is_dict_and_has_key(obj, key):
    """Check if the given object is a dict which contains the given key."""
    if isinstance(obj, dict):
        return obj.has_key(key)
    return False

class Decoder(object):
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

    def get_key(self, obj):
        """Returns a unique key for the given object."""
        if is_dict_and_has_key(obj, "_nova_classname"):
            table_name = obj["_nova_classname"]
            key = obj["id"]
            return "%s_%s_%s" % (table_name, str(key), self.request_uuid)
        elif is_dict_and_has_key(obj, "novabase_classname"):
            table_name = obj["novabase_classname"]
            key = obj["id"]
            return "%s_%s_%s" % (table_name, str(key), self.request_uuid)
        else:
            return "%s_%s_%s" % (hex(id(obj)), hex(id(obj)), self.request_uuid)

    def novabase_desimplify(self, obj):
        """Desimplify a novabase object."""
        key = self.get_key(obj)
        if not self.cache.has_key(key):
            can_load = True
            if obj.has_key("_nova_classname"):
                tablename = obj["_nova_classname"]
            elif obj.has_key("novabase_classname"):
                tablename = models.get_tablename_from_name(
                    obj["novabase_classname"]
                )
            else:
                tablename = models.get_tablename_from_name(
                    obj["_metadata_novabase_classname"]
                )
            if "simplify_strategy" in obj:
                can_load = False
            self.cache[key] = lazy_reference.LazyReference(
                tablename,
                obj["id"],
                deconverter=self,
                request_uuid=self.request_uuid
            )
            if can_load:
                self.cache[key].load(obj)
        return self.cache[key]

    def datetime_desimplify(self, value):
        """Desimplify a datetime object."""
        result = datetime.datetime.strptime(value["value"], DATE_FORMAT)
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
        if is_dict_and_has_key(obj, "simplify_strategy"):
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
        elif is_dict and obj.has_key("_metadata_novabase_classname"):
            result = self.novabase_desimplify(obj)
        elif is_dict:
            result = {}
            for item in obj:
                result[item] = self.desimplify(obj[item])
        return result
