"""LazyReference module.

This module contains functions, classes and mix-in that are used for the
building lazy references to objects located in database. These lazy references
will be evaluated only when some functions or properties will be called.

"""

import uuid
import time

import models
import lib.rome.driver.database_driver as database_driver

# RIAK_CLIENT = riak.RiakClient(pb_port=8087, protocol='pbc')


def now_in_ms():
    return int(round(time.time() * 1000))


class EmptyObject:
    pass


class LazyReference:
    """Class that references a remote object stored in database. This aims
    easing the development of algorithm on relational objects: instead of
    populating relationships even when not required, we load them "only" when
    it is used!"""

    def __init__(self, base, id, request_uuid, desimplifier):
        """Constructor"""

        from lib.rome.core.dataformat import deconverter

        caches = deconverter.CACHES

        self.base = base
        self.id = id
        self.version = -1

        self.request_uuid = request_uuid if request_uuid is not None else uuid.uuid1()
        if not caches.has_key(self.request_uuid):
            caches[self.request_uuid] = {}
        self.cache = caches[self.request_uuid]

        if deconverter is None:
            from lib.rome.core.dataformat.deconverter import JsonDeconverter

            self.desimplifier = JsonDeconverter(request_uuid=request_uuid)
        else:
            self.desimplifier = deconverter

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

        if "novabase_classname" in obj:
            model_class_name = obj["novabase_classname"]
        elif "metadata_novabase_classname" in obj:
            model_class_name = obj["metadata_novabase_classname"]

        if model_class_name is not None:
            model = models.get_model_class_from_name(model_class_name)
            model_object = model()
            if not self.cache.has_key(key):
                self.cache[key] = model_object
            return self.cache[key]
        else:
            return None

    def update_nova_model(self, obj):
        """Update the fields of the given object."""

        key = self.get_key()
        current_model = self.cache[key]

        # Check if obj is simplified or not
        if "simplify_strategy" in obj:
            obj = database_driver.get_driver().get(obj["tablename"], obj["id"])

        # For each value of obj, set the corresponding attributes.
        for key in obj:
            simplified_value = self.desimplifier.desimplify(obj[key])
            try:
                if simplified_value is not None:
                    setattr(
                        current_model,
                        key,
                        self.desimplifier.desimplify(obj[key])
                    )
                else:
                    setattr(current_model, key, obj[key])
            except Exception as e:
                if "None is not list-like" in str(e):
                    setattr(current_model, key, [])
                else:
                    pass

        if hasattr(current_model, "user_id") and obj.has_key("user_id"):
            current_model.user_id = obj["user_id"]

        if hasattr(current_model, "project_id") and obj.has_key("project_id"):
            current_model.project_id = obj["project_id"]

        # Update foreign keys
        current_model.update_foreign_keys(self.request_uuid)

        return current_model

    def load(self):
        """Load the referenced object from the database. The result will be
        cached, so that next call will not create any database request."""

        self.version = 0

        key = self.get_key()

        obj = database_driver.get_driver().get(self.base, self.id)

        self.spawn_empty_model(obj)
        self.update_nova_model(obj)

        return self.cache[key]

    def get_complex_ref(self):
        """Return the python object that corresponds the referenced object. The
        first time this method has been invocked, a request to the database is
        made and the result is cached. The next times this method is invocked,
        the previously cached result is returned."""

        key = self.get_key()

        if not self.cache.has_key(key):
            self.load()

        return self.cache[key]


    def __getattr__(self, item):
        """This method 'intercepts' call to attribute/method on the referenced
        object: the object is thus loaded from database, and the requested
        attribute/method is then returned."""

        return getattr(self.get_complex_ref(), item)

    def __setattr__(self, name, value):
        """This method 'intercepts' affectation to attribute/method on the
        referenced object: the object is thus loaded from database, and the
        requested attribute/method is then setted with the given value."""

        if name in ["base", "id", "cache", "desimplifier", "request_uuid",
                    "uuid", "version"]:
            self.__dict__[name] = value
        else:
            setattr(self.get_complex_ref(), name, value)
            self.version += 1
            return self


    def __str__(self):
        """This method prevents the loading of the remote object when a
        LazyReference is printed."""

        return "Lazy(%s:%s:%d)" % (self.get_key(), self.base, self.version)

    def __repr__(self):
        """This method prevents the loading of the remote object when a
        LazyReference is printed."""

        return "Lazy(%s:%s:%d)" % (self.get_key(), self.base, self.version)

    def __nonzero__(self):
        """This method is required by some services of OpenStack."""

        return not not self.get_complex_ref()
