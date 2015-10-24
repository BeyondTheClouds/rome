"""Models module.

This module contains a set of class and functions to enable the definition of
models in the same way as SQLALCHEMY do.

"""

import uuid
import sys
import datetime
import logging

from lib.rome.core.dataformat import get_decoder, get_encoder
import lib.rome.driver.database_driver as database_driver
from oslo.db.sqlalchemy import models
import utils



def starts_with_uppercase(name):
    if name is None or len(name) == 0:
        return False
    else:
        return name[0].isupper()


def convert_to_model_name(name):
    result = name
    if result[-1] == "s":
        result = result[0:-1]
    if not starts_with_uppercase(result):
        result = result[0].upper() + result[1:]
    if result == "InstanceActionsEvent":
        result = "InstanceActionEvent"
    return result


def get_model_classname_from_tablename(tablename):
    for klass in sys.rome_global_scope:
        if klass.__tablename__ == tablename:
            return klass.__name__
    return None

def get_model_tablename_from_classname(classname):
    return get_model_class_from_name(classname).__tablename__


def get_model_class_from_name(name):
    corrected_name = convert_to_model_name(name)
    for klass in sys.rome_global_scope:
        if klass.__name__ == corrected_name or klass.__name__ == name:
            return klass
    return None


def get_tablename_from_name(name):
    model = get_model_class_from_name(name)
    return model.__tablename__


def same_version(a, b, model):
    if a is None:
        return False
    for attr in model._sa_class_manager:
        if "RelationshipProperty" in str(type(model._sa_class_manager[attr].property)):
            continue
        if (a.has_key(attr) ^ b.has_key(attr)) or (a.has_key(attr) and a[attr] != b[attr]):
            return False
    return True


def merge_dict(a, b):
    result = {}
    if a is not None:
        for key in a:
            result[key] = a[key]
    if b is not None:
        for key in b:
            value = b[key]
            result[key] = value
    return result

def global_scope(cls):
    if not hasattr(sys, "rome_global_scope"):
        setattr(sys, "rome_global_scope", [])
    sys.rome_global_scope += [cls]
    return cls

class IterableModel(object):
    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __contains__(self, key):
        return hasattr(self, key)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __iter__(self):
        for field in self._sa_class_manager:
            yield field

class Entity(models.ModelBase, IterableModel, utils.ReloadableRelationMixin):

    def __init__(self):
        self._session = None
        self.rome_version_number = -1

    def already_in_database(self):
        return hasattr(self, "id") and (self.id is not None)

    def soft_delete(self, session=None):
        # <SOFT DELETE IMPLEMENTATION>
        self.deleted = True
        object_converter_datetime = get_encoder()
        self.deleted_at = object_converter_datetime.simplify(datetime.datetime.utcnow())
        if session is not None:
            session.add(self)
            return
        else:
            self.save()
        # </SOFT DELETE IMPLEMENTATION>

        # # <HARD DELETE IMPLEMENTATION>
        # if session is not None:
        #     session.delete(self)
        #     return
        # database_driver.get_driver().remove_key(self.__tablename__, self.id)
        # # </HARD DELETE IMPLEMENTATION>

    def update(self, values, synchronize_session='evaluate', request_uuid=uuid.uuid1(), do_save=True, skip_session=False):
        """Set default values"""
        try:
            for field in self._sa_class_manager:
                instance_state = self._sa_instance_state
                field_value = getattr(self, field)
                if field_value is None:
                    try:
                        field_column = instance_state.mapper._props[field].columns[0]
                        field_name = field_column.name
                        field_default_value = field_column.default.arg

                        if not "function" in str(type(field_default_value)):
                            setattr(self, field_name, field_default_value)
                    except:
                        pass
        except:
            pass
        for key in values:
            value = values[key]
            try:
                setattr(self, key, value)
            except Exception as e:
                logging.error(e)
                pass
        self.update_foreign_keys()
        # self.load_relationships()
        return self

    def register_associated_object(self, obj):
        if not hasattr(self, "_associated_objects"):
            setattr(self, "_associated_objects", [])
        associated_objects = getattr(self, "_associated_objects")
        associated_objects = [obj] + associated_objects
        setattr(self, "_associated_objects", associated_objects)

    def get_associated_objects(self):
        return getattr(self, "_associated_objects", [])

    def reset_associated_objects(self):
        return setattr(self, "_associated_objects", [])

    def save(self, session=None, request_uuid=uuid.uuid1(), force=False, no_nested_save=False, increase_version=True, already_saved=None):

        if already_saved is None:
            already_saved = []

        if session is not None:
            session.add(self)
            return

        self.update_foreign_keys()

        target = self
        table_name = self.__tablename__

        """Check if the current object has an value associated with the "id"
        field. If this is not the case, following code will generate an unique
        value, and store it in the "id" field."""
        if not self.already_in_database():
            self.id = database_driver.get_driver().next_key(table_name)
            logging.debug("booking the id %s in table %s" % (self.id, self.__tablename__))

        """Before keeping the object in database, we simplify it: the object is
        converted into "JSON like" representation, and nested objects are
        extracted. It results in a list of object that will be stored in the
        database."""
        object_converter = get_encoder(request_uuid)
        object_converter.simplify(self)

        current_id = "%s@%s" % (self.id, self.__tablename__)
        if current_id in already_saved:
            return
        already_saved += [current_id]

        candidates = []

        # Handle relationships's objects: they may be saved!
        def is_unmodified(v):
            return hasattr(relationship_value, "is_relationship_list") and getattr(relationship_value, "is_loaded", False)

        for rel_field in self.get_relationship_fields():
            attr = getattr(self, rel_field)

            if hasattr(attr, "is_loaded") and getattr(attr, "is_loaded"):
                if attr.wrapped_value is not None:
                    if attr.is_relationship_list:
                        candidates += attr.wrapped_value
                    else:
                        candidates = [attr.wrapped_value]
            if len(candidates) == 0:
                corresponding_relationship = filter(lambda x: x.local_fk_field == rel_field, self.get_relationships())
                for rel in corresponding_relationship:
                    if rel.direction in ["MANYTOONE", "ONETOMANY"] and getattr(self, rel.local_object_field) is not None:
                        relationship_value = getattr(self, rel.local_object_field)
                        # if is_unmodified_value(relationship_value):
                        # relationship_value = relationship_value.data
                        # if relationship_value is not None:
                        prefiltered_candidates = []
                        if rel.is_list:
                            for each_value in relationship_value:
                                prefiltered_candidates += [each_value]
                        else:
                            prefiltered_candidates += [relationship_value]
                        filtered_candidates = filter(lambda x: not is_unmodified(x), prefiltered_candidates)
                        candidates += map(lambda x: getattr(x, "data", x), filtered_candidates)
        # Handle associated objects: they may be saved!
        for associated_object in self.get_associated_objects():
            candidates += [associated_object]

        # As every associated_object are going to be saved, associated_objects may be reset
        self.reset_associated_objects()

        for c in candidates:
            try:
                c.save(request_uuid=request_uuid, force=force, no_nested_save=no_nested_save, increase_version=increase_version, already_saved=already_saved)
            except:
                pass

        key = object_converter.get_cache_key(self)

        classname = get_model_classname_from_tablename(self.__tablename__)
        table_name = get_model_tablename_from_classname(classname)

        current_object = object_converter.complex_cache[key]
        current_object["nova_classname"] = self.__tablename__

        if not "id" in current_object or current_object["id"] is None:
            current_object["id"] = self.next_key(table_name)
        else:
            model_class = get_model_class_from_name(classname)
            existing_object = database_driver.get_driver().get(table_name, current_object["id"])

            if not same_version(existing_object, current_object, model_class):
                current_object = merge_dict(existing_object, current_object)
            else:
                return

        if current_object["id"] == -1:
            logging.debug("skipping the storage of object %s" % (current_object["id"]))
            return

        object_converter_datetime = get_encoder(request_uuid)

        if (current_object.has_key("created_at") and current_object[
            "created_at"] is None) or not current_object.has_key("created_at"):
            current_object["created_at"] = object_converter_datetime.simplify(datetime.datetime.utcnow())
        current_object["updated_at"] = object_converter_datetime.simplify(datetime.datetime.utcnow())

        logging.debug("starting the storage of %s (uuid=%s, session=%s)" % (current_object, request_uuid, session))

        try:
            local_object_converter = get_encoder(request_uuid)
            corrected_object = local_object_converter.simplify(current_object)
            if target.__tablename__ == corrected_object["nova_classname"] and target.id == corrected_object["id"]:
                corrected_object["session"] = getattr(target, "session", None)
            if increase_version:
                if "rome_version_number" in corrected_object:
                    self.rome_version_number = corrected_object["rome_version_number"]
                if hasattr(self, "rome_version_number"):
                    self.rome_version_number += 1
                else:
                    self.rome_version_number = 0
            corrected_object["rome_version_number"] = self.rome_version_number
            database_driver.get_driver().put(table_name, current_object["id"], corrected_object, secondary_indexes=getattr(model_class, "_secondary_indexes", []))
            database_driver.get_driver().add_key(table_name, current_object["id"])
        except Exception as e:
            import traceback
            traceback.print_exc()
            logging.error("Failed to store following object: %s because of %s, becoming %s" % (
            current_object, e, corrected_object))
            pass
        logging.debug("finished the storage of %s" % (current_object))
        return self