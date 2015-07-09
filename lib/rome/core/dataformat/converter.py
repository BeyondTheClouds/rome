"""Simplifier module.

This module contains functions, classes and mix-in that are used for the
simplification of objects, before storing them into the discovery database.

"""

import lib.rome.core.utils as utils
import uuid
from lib.rome.utils.LimitedSizeDictionnary import LimitedSizeDictionnary

SIMPLE_CACHES = LimitedSizeDictionnary(size_limit=20)
COMPLEX_CACHES = LimitedSizeDictionnary(size_limit=20)
TARGET_CACHES = LimitedSizeDictionnary(size_limit=20)


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


class JsonConverter(object):
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

        key = self.get_cache_key(obj)
        return self.simple_cache.has_key(key)

    def datetime_simplify(self, datetime_ref):
        """Simplify a datetime object."""

        return {
            "simplify_strategy": "datetime",
            "value": datetime_ref.strftime('%b %d %Y %H:%M:%S'),
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
        if fields_iterator is not None:
            for field in fields_iterator:
                field_value = getattr(obj, field)

                if utils.is_novabase(field_value):
                    complex_object[field] = self.process_field(field_value)
                elif isinstance(field_value, list):
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

            obj.update_foreign_keys()
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
                    "pid": extract_adress(obj)
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
            complex_object["metadata_novabase_classname"] = metadata_class_name
            complex_object["pid"] = extract_adress(obj)
            complex_object["rid"] = str(self.request_uuid)

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
                        "metadata_novabase_classname": metadata_class_name,
                        "pid": extract_adress(obj),
                        "rid": str(self.request_uuid)
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
