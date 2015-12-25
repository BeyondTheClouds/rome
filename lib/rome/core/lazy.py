"""LazyReference module.

This module contains functions, classes and mix-in that are used for the
building lazy references to objects located in database. These lazy references
will be evaluated only when some functions or properties will be called.

"""

import uuid
import time

import models
import lib.rome.driver.database_driver as database_driver
import traceback

from lib.rome.core.dataformat import get_decoder

def now_in_ms():
    return int(round(time.time() * 1000))


class EmptyObject:
    pass

class LazyAttribute(dict):
    """This class is used to intercept calls to emit_backref. This enables to have efficient lazy loading."""
    def __getitem__(self, item):
        return self

    def __getattr__(self, item):
        """This method 'intercepts' call to attribute/method."""
        if item in ["append"]:
            return self.append
        if item in ["pop"]:
            return self.pop
        if item in ["delete"]:
            return self.delete
        return self

    def append(self, *args, **kwargs):
        pass

    def pop(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass

class LazyBackrefBuffer(object):
    """This class intercepts calls to emit_backref. This enables to have efficient lazy loading."""
    def __init__(self):
        self.attributes = []

    def __getattr__(self, item):
        """This method 'intercepts' call to attribute/method."""
        if item in ["manager", "parents"]:
            attribute = LazyAttribute()
            self.attributes += [attribute]
            return LazyAttribute()
        return getattr(self, item)


class LazyDate:
    """Class that represents a "Lazyfied" date value. The LazyValue wraps a value in a dict format, and
    when an external object accesses one of the wrapped date_dict, content of the dict is "converted" in an object format
    (models entities). In a few words LazyDate(date_dict) <-> JsonDeconverter(date_dict) ."""

    def __init__(self, wrapped_date_dict, request_uuid):
        from lib.rome.core.dataformat import get_decoder
        self.deconverter = get_decoder(request_uuid=request_uuid)
        self.wrapped_date_dict = wrapped_date_dict
        self.wrapped_value = None
        self.request_uuid = request_uuid

    # def transform(self, x):
    #     return self.deconverter.desimplify(x)

    def __repr__(self):
        return "LazyDate(...)"
        self.lazy_load()
        return str(self.wrapped_value)

    def lazy_load(self):
        if self.wrapped_value is None:
            self.wrapped_value = self.deconverter.desimplify(self.wrapped_date_dict)

    def __getattr__(self, attr):
        self.lazy_load()
        return getattr(self.wrapped_value, attr)

    def __setattr__(self, key, value):
        if key in ["deconverter", "wrapped_date_dict", "wrapped_value", "request_uuid"]:
            self.__dict__[key] = value
        else:
            self.lazy_load()
            setattr(self.wrapped_value, key, value)
        pass

class LazyValue:
    """Class that represents a "Lazyfied" value. The LazyValue wraps a value in a dict format, and
    when an external object accesses one of the wrapped dict, content of the dict is "converted" in an object format
    (models entities). In a few words LazyValue(dict).id <-> JsonDeconverter(dict).id ."""

    def __init__(self, wrapped_dict, request_uuid):
        self.deconverter = None
        self.wrapped_dict = wrapped_dict
        self.wrapped_value = None
        self.request_uuid = request_uuid

        """ Remove Rome's specific attributes of the dict, to stick with what SQLAlchemy returns in order to pass
        Nova's unit tests! """
        self._unwanted_keys = ["_session", "_nova_classname", "simplify_strategy", "_rome_version_number", "_rid", "_pid", "_metadata_novabase_classname", "_diff_dict", "_unwanted_keys"]
        self._diff_dict = {}

        if not type(wrapped_dict) is dict:
            candidate_value = wrapped_dict
            self.lazy_load()
            self.wrapped_dict = {"value": candidate_value}
        # self._filter_wrapped_dict()

    def _filter_wrapped_dict(self):
        for key in self._unwanted_keys:
            v = self.wrapped_dict[key]
            self._diff_dict[key] = v

    def get_filtered_dict(self):
        result = {}
        filtered_keys = filter(lambda x: x not in self._unwanted_keys, self.wrapped_dict.keys())
        for key in filtered_keys:
            result[key] = self.wrapped_dict[key]
        return result

    def transform(self, x):
        if self.deconverter is None:
            self.deconverter = get_decoder(request_uuid=self.request_uuid)
        return self.deconverter.desimplify(x)

    def get_relationships(self, foreignkey_mode=False):
        from utils import get_relationships
        obj = self.wrapped_value.get_complex_ref()
        return get_relationships(obj, foreignkey_mode=foreignkey_mode)

    def load_relationships(self, debug=True):
        """Update foreign keys according to local fields' values."""
        from utils import LazyRelationship
        if not hasattr(self.wrapped_value, "get_complex_ref"):
            self.lazy_load()
        wv = self.wrapped_value
        # Find relationships and load them
        attrs = wv.get_complex_ref().__dict__ if hasattr(wv, "get_complex_ref") else []
        if len(attrs) > 0:
            for rel in self.get_relationships(foreignkey_mode=True):
                key = rel.local_object_field
                if not rel.is_list and key in attrs and attrs[key] is not None:
                    continue
                if rel.is_list and key in attrs and "InstrumentedList" not in str(type(attrs[key])):
                    continue
                attrs[key] = LazyRelationship(rel, request_uuid=self.request_uuid)
        pass

    def __repr__(self):
        # return """{"type": "LazyValue"}"""
        if not type(self.wrapped_dict) is dict:
            return str(self.wrapped_value)
        if "timezone" in self.wrapped_dict:
            return ""
        return str(self.get_filtered_dict())

    def get_key(self):
        """Returns a unique key for the current LazyReference."""
        return "%s_%s" % (self.resolve_model_name(), str(self.id))

    def resolve_model_name(self):
        """Returns the model class corresponding to the remote object."""
        if "_metadata_novabase_classname" in self.wrapped_dict:
            return self.wrapped_dict["_metadata_novabase_classname"]
        elif self.wrapped_value is not None:
            return models.get_model_classname_from_tablename(self.wrapped_value.base)
        else:
            return "None"

    def lazy_load(self):
        if self.wrapped_value is None:
            if self.deconverter is None:
                self.deconverter = get_decoder(request_uuid=self.request_uuid)
            self.wrapped_value = self.deconverter.desimplify(self.wrapped_dict)
            self.load_relationships()

    def __getattr__(self, attr):
        if attr in self.wrapped_dict:
            value = self.wrapped_dict[attr]
            if type(value) is dict and "timezone" in value:
                if self.deconverter is None:
                    self.deconverter = get_decoder(request_uuid=self.request_uuid)
                return self.deconverter.desimplify(value)
            return value
        self.lazy_load()
        # if "_nova_classname" in self.wrapped_dict and "aggregate" in self.wrapped_dict["_nova_classname"]:
        return getattr(self.wrapped_value, attr)

    def __setattr__(self, key, value):
        if key in ["deconverter", "wrapped_dict", "wrapped_value", "request_uuid", "_unwanted_keys", "_diff_dict"]:
            self.__dict__[key] = value
        else:
            self.lazy_load()
            obj = self.__dict__["wrapped_value"].get_complex_ref()
            self.__dict__["wrapped_dict"][key] = value
            obj.__dict__[key] = value
            if obj.is_relationship_field(key):
                obj.handle_relationship_change_event(key, value)
        pass


class LazyReference:
    """Class that references a remote object stored in database. This aims
    easing the development of algorithm on relational objects: instead of
    populating relationships even when not required, we load them "only" when
    it is used!"""

    def __init__(self, base, id, request_uuid, deconverter):
        """Constructor"""
        from lib.rome.core.dataformat import json as json_module
        caches = json_module.CACHES
        self.base = base
        self.id = id
        self.version = -1
        self.lazy_backref_buffer = LazyBackrefBuffer()
        self.request_uuid = request_uuid if request_uuid is not None else uuid.uuid1()
        if self.request_uuid not in caches:
            caches[self.request_uuid] = {}
        self.cache = caches[self.request_uuid]
        if deconverter is None:
            from lib.rome.core.dataformat import get_decoder
            self.deconverter = get_decoder(request_uuid=request_uuid)
        else:
            self.deconverter = deconverter
        self._session = None

    def set_session(self, session):
        self._session = session
        key = self.get_key()
        if key in self.cache:
            self.cache[key]._session = session

    def get_key(self):
        """Returns a unique key for the current LazyReference."""
        return "%s_%s" % (self.resolve_model_name(), str(self.id))

    def resolve_model_name(self):
        """Returns the model class corresponding to the remote object."""
        return models.get_model_classname_from_tablename(self.base)

    def spawn_empty_model(self, obj):
        """Spawn an empty instance of the model class specified by the
        given object"""
        key = self.get_key()
        if obj is not None:
            if"novabase_classname" in obj:
                model_class_name = obj["novabase_classname"]
            elif "_metadata_novabase_classname" in obj:
                model_class_name = obj["_metadata_novabase_classname"]
        else:
            model_class_name = self.resolve_model_name()
        if model_class_name is not None:
            model = models.get_model_class_from_name(model_class_name)
            model_object = model()
            model_object.deleted = False
            if key not in self.cache:
                self.cache[key] = model_object
            return self.cache[key]
        else:
            return None

    def update_nova_model(self, obj):
        """Update the fields of the given object."""
        key = self.get_key()
        current_model = self.cache[key]

        if obj is None:
            return current_model

        # Check if obj is simplified or not
        if "simplify_strategy" in obj:
            obj = database_driver.get_driver().get(obj["tablename"], obj["id"])
        # For each value of obj, set the corresponding attributes.
        for key in obj:
            simplified_value = self.deconverter.desimplify(obj[key])
            try:
                if simplified_value is not None:
                    value = self.deconverter.desimplify(obj[key])
                    current_model[key] = value
                else:
                    current_model[key] = obj[key]
            except Exception as e:
                if "None is not list-like" in str(e):
                    setattr(current_model, key, [])
                else:
                    traceback.print_exc()
                    pass
        if hasattr(current_model, "user_id") and obj.has_key("user_id"):
            current_model.user_id = obj["user_id"]
        if hasattr(current_model, "project_id") and obj.has_key("project_id"):
            current_model.project_id = obj["project_id"]
        return current_model

    def load(self, data=None):
        """Load the referenced object from the database. The result will be
        cached, so that next call will not create any database request."""
        self.version = 0
        key = self.get_key()
        first_load = data is None
        if first_load:
            data = database_driver.get_driver().get(self.base, self.id)
        self.spawn_empty_model(data)
        self.update_nova_model(data)
        # if first_load and "aggregate" in self.base:
        if first_load:
            self.get_complex_ref().load_relationships()
            # self.update_nova_model(data)
        if self._session is not None:
            self.cache[key]._session = self._session
        return self.cache[key]

    def get_complex_ref(self):
        """Return the python object that corresponds the referenced object. The
        first time this method has been invocked, a request to the database is
        made and the result is cached. The next times this method is invocked,
        the previously cached result is returned."""
        key = self.get_key()
        if not key in self.cache:
            self.load()
        return self.cache[key]

    def __getattr__(self, item):
        """This method 'intercepts' call to attribute/method on the referenced
        object: the object is thus loaded from database, and the requested
        attribute/method is then returned."""
        if item == "_sa_instance_state":
            key = self.get_key()
            if not self.cache.has_key(key):
                return self.lazy_backref_buffer
        return getattr(self.get_complex_ref(), item)

    def __setattr__(self, name, value):
        """This method 'intercepts' affectation to attribute/method on the
        referenced object: the object is thus loaded from database, and the
        requested attribute/method is then setted with the given value."""
        if name in ["base", "id", "cache", "deconverter", "request_uuid",
                    "uuid", "version", "lazy_backref_buffer", "_session", "_version"]:
            self.__dict__[name] = value
        else:
            setattr(self.get_complex_ref(), name, value)
            if self._session is not None:
                self._session.add(self)
            return self

    def __str__(self):
        """This method prevents the loading of the remote object when a
        LazyReference is printed."""
        return "Lazy(%s:%s:%d)" % (self.get_key(), self.base, self.version)

    def __repr__(self):
        """This method prevents the loading of the remote object when a
        LazyReference is printed."""
        return "Lazy(%s:%s:%d)" % (self.get_key(), self.base, self.version)

    def __hash__(self):
        """This method prevents the loading of the remote object when a
        LazyReference is stored in a dict."""
        return self.__str__().__hash__()

    def __nonzero__(self):
        """This method is required by some services of OpenStack."""
        return not not self.get_complex_ref()
