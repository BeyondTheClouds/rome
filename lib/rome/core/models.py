"""Models module.

This module contains a set of class and functions to enable the definition of
models in the same way as SQLALCHEMY do.

"""

import uuid
import sys
import datetime
import logging
import types

from sqlalchemy import Column
from lib.rome.core.dataformat import get_decoder, get_encoder
import lib.rome.driver.database_driver as database_driver
# from oslo.db.sqlalchemy import models
from lib.rome.core.model_base import ModelBase
import utils

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

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

entity_relationship_field = {}

class Entity(ModelBase, IterableModel, utils.ReloadableRelationMixin):

    __table_args__ = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8'}
    __table_initialized__ = False
    __protected_attributes__ = set([
        "created_at", "updated_at", "deleted_at", "deleted"])

    def keys(self):
        filtered_keys = ["_sa_instance_state"]
        return filter(lambda x: x not in filtered_keys, self.__dict__.keys())

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def to_dict(self):
        d = self.__dict__.copy()
        # NOTE(flaper87): Remove
        # private state instance
        # It is not serializable
        # and causes CircularReference
        d.pop("_sa_instance_state")
        return d

    def __init__(self):
        self._session = None
        self._rome_version_number = -1

    def _set_default_values(self, skip_non_none=True):
        attributes = self.__class__.__dict__
        for attr_name, attr_value in attributes.iteritems():
            if hasattr(attr_value, "default") and getattr(attr_value, "default"):
                default_value = attr_value.default.arg
                if skip_non_none and getattr(self, attr_name, None):
                    continue
                if isinstance(default_value, types.FunctionType):
                    default_value = attr_value.default.arg(self)
                setattr(self, attr_name, default_value)

    def __setitem__(self, key, value):
        """ This function overrides the default __setitem_ provided by sqlalchemy model class, in order to prevent
        triggering of events that loads relationships, when a relationship field is set. Instead, the relationship
        is handle by this method.

        :param key: a string value representing the key
        :param value: an object
        :return: nothing
        """
        self.__dict__[key] = value
        if self.is_relationship_field(key):
            self.handle_relationship_change_event(key, value)

    def __setattr__(self, key, value):
        """ This function overrides the default __setattr_ provided by sqlalchemy model class, in order to prevent
        triggering of events that loads relationships, when a relationship field is set. Instead, the relationship
        is handle by this method.

        :param key: a string value representing the key
        :param value: an object
        :return: nothing
        """
        self.__dict__[key] = value
        if self.is_relationship_field(key):
            self.handle_relationship_change_event(key, value)

    def is_relationship_field(self, key):
        tablename = self.__tablename__
        if not tablename in entity_relationship_field:
            fields = self.get_relationship_fields(with_indirect_field=True)
            entity_relationship_field[tablename] = fields
        return key in entity_relationship_field[tablename]

    def handle_relationship_change_event(self, key, value):
        relationships = filter(lambda x: key in [x.local_object_field, x.local_fk_field], self.get_relationships())
        for r in relationships:
            if key == r.local_fk_field:
                if r.direction in ["MANYTOONE"]:
                    self.load_relationships(filter_keys=[r.local_object_field])
                elif r.direction in ["ONETOMANY"]:
                    existing_value = getattr(self, r.local_object_field)
                    candidates = existing_value if r.is_list else [existing_value]
                    for c in candidates:
                        if c is not None:
                            existing_fk_value = getattr(c, r.remote_object_field, None)
                            if existing_fk_value is None:
                                setattr(c, r.remote_object_field, value)
                else:
                    try:
                        v = getattr(getattr(self, r.local_object_field), r.remote_object_field)
                        self.__dict__[key] = v
                    except:
                        pass
                    self.load_relationships(filter_keys=[r.local_object_field])
            else:
                if r.direction in ["MANYTOONE"]:
                    if key == r.local_object_field and not r.is_list:
                        self.__dict__[r.local_fk_field] = getattr(value, r.remote_object_field, None)
                        if hasattr(value, "get_relationships"):
                            for r2 in value.get_relationships():
                                if r2.remote_object_tablename == self.__tablename__:
                                    if r2.direction in ["ONETOMANY"]:
                                        existing_value = getattr(value, r2.local_object_field, None)
                                        if existing_value is None or hasattr(existing_value, "is_list"):
                                            setattr(value, r2.local_object_field, self)
                            if hasattr(value, "register_associated_object"):
                                value.register_associated_object(self)
                if r.direction in ["ONETOMANY"]:
                    candidates = value if r.is_list else [value]
                    filtered_candidates = filter(lambda x: hasattr(x, "get_relationships"), candidates)
                    for c in filtered_candidates:
                        for r2 in c.get_relationships():
                            if r2.direction in ["MANYTOONE"]:
                                setattr(c, r2.local_fk_field, getattr(self, r2.remote_object_field))
                                setattr(c, r2.local_object_field, self)

    def already_in_database(self):
        return hasattr(self, "id") and (self.id is not None)

    def delete(self, session=None):
        # <HARD DELETE IMPLEMENTATION>
        if session is not None:
            session.delete(self)
            return
        database_driver.get_driver().remove_key(self.__tablename__, self.id)
        # </HARD DELETE IMPLEMENTATION>

    def soft_delete(self, session=None):
        # <SOFT DELETE IMPLEMENTATION>
        self.deleted = 1
        object_converter_datetime = get_encoder()
        self.deleted_at = object_converter_datetime.simplify(datetime.datetime.utcnow())
        if session is not None:
            session.add(self)
            return
        else:
            self.save()

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
        # self.update_foreign_keys()
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

    def to_dict(self):
        d = self.__dict__.copy()
        # NOTE(flaper87): Remove
        # private state instance
        # It is not serializable
        # and causes CircularReference
        d.pop('_sa_instance_state')
        return d

    def reset_associated_objects(self):
        return setattr(self, "_associated_objects", [])

    def _fill_none_field(self, db_object):
        """
        Fill self with None value.
        A column with no default value and no value given will be persisted
        in the DB with a null value, but the entity will not reflect this.
        The purpose of this method is to force attributes in the entity to be set
        to None before returning.
        :param db_object: the object that has been persisted in the DB
        :return:
        """
        for k, v in db_object.iteritems():
            if v is None and hasattr(self, k):
                setattr(self, k, None)

    def save(self, session=None, request_uuid=uuid.uuid1(), force=False, no_nested_save=False, increase_version=True, session_saving=None):

        if session is not None and not force:
            session.add(self)
            session.flush()
            return

        object_key = "%s:%s" % (self.__tablename__, self.id)

        if session_saving and self.id and object_key in session_saving.already_saved:
            return

        # self.update_foreign_keys()

        target = self
        table_name = self.__tablename__

        self._set_default_values()

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

        saving_candidates = object_converter.complex_cache

        if no_nested_save:
            key = object_converter.get_cache_key(self)
            saving_candidates = {
                key: saving_candidates[key]
            }

        for key in [key for key in saving_candidates if "x" in key]:

            candidate = saving_candidates[key]
            default_classname = "_".join(key.split("_")[0:-1])
            classname = candidate.get("_metadata_novabase_classname", default_classname)
            table_name = get_model_tablename_from_classname(classname)

            simplified_object = object_converter.simple_cache[key]
            complex_object = object_converter.complex_cache[key]
            target_object = object_converter.target_cache[key]

            if simplified_object["id"] is not None:
                continue

            """Find a new_id for this object"""
            new_id = database_driver.get_driver().next_key(table_name)

            """Assign this id to the object"""
            simplified_object["id"] = new_id
            complex_object["id"] = new_id
            target_object.id = new_id

            pass

        for key in saving_candidates:

            candidate = saving_candidates[key]
            default_classname = "_".join(key.split("_")[0:-1])
            classname = candidate.get("_metadata_novabase_classname", default_classname)
            table_name = get_model_tablename_from_classname(classname)

            current_object = object_converter.complex_cache[key]

            current_object["_nova_classname"] = table_name

            if not "id" in current_object or current_object["id"] is None:
                current_object["id"] = self.next_key(table_name)
            else:

                current_object_key = "%s:%s" % (table_name, current_object["id"])

                if session_saving and current_object_key in session_saving.already_saved:
                    continue

                model_class = get_model_class_from_name(classname)
                existing_object = database_driver.get_driver().get(table_name, current_object["id"])

                if not same_version(existing_object, current_object, model_class):
                    current_object = merge_dict(existing_object, current_object)
                else:
                    continue

            if current_object["id"] == -1:
                logging.debug("skipping the storage of object %s" % (current_object["id"]))
                # continue
                break

            object_converter_datetime = get_encoder(request_uuid)

            current_time = datetime.datetime.utcnow()
            if (current_object.has_key("created_at") and current_object[
                "created_at"] is None) or not current_object.has_key("created_at"):
                current_object["created_at"] = object_converter_datetime.simplify(current_time)
            current_object["updated_at"] = object_converter_datetime.simplify(current_time)
            self.updated_at = current_time

            logging.debug("starting the storage of %s" % (current_object))

            try:
                local_object_converter = get_encoder(request_uuid)
                corrected_object = local_object_converter.simplify(current_object)
                if target.__tablename__ == corrected_object["_nova_classname"] and target.id == corrected_object["id"]:
                    corrected_object["_session"] = getattr(target, "_session", None)
                if increase_version:
                    if "_rome_version_number" in corrected_object:
                        self._rome_version_number = corrected_object["_rome_version_number"]
                    if hasattr(self, "_rome_version_number"):
                        self._rome_version_number += 1
                    else:
                        self._rome_version_number = 0
                corrected_object["_rome_version_number"] = self._rome_version_number
                database_driver.get_driver().put(table_name, current_object["id"], corrected_object, secondary_indexes=getattr(model_class, "_secondary_indexes", []))
                database_driver.get_driver().add_key(table_name, current_object["id"])
            except Exception as e:
                import traceback
                traceback.print_exc()
                logging.error("Failed to store following object: %s because of %s, becoming %s" % (
                current_object, e, corrected_object))
                pass
            logging.debug("finished the storage of %s" % (current_object))

            if session_saving:
                session_saving.already_saved += [current_object_key]
        # self.load_relationships()

        candidates = []
        # Handle associated objects: they may be saved!
        for associated_object in self.get_associated_objects():
            candidates += [associated_object]

        # As every associated_object are going to be saved, associated_objects may be reset
        self.reset_associated_objects()

        for c in candidates:
            try:
                # object_converter.simplify(c)
                c.save(request_uuid=request_uuid, force=force, no_nested_save=no_nested_save, increase_version=increase_version, session_saving=session_saving)
            except:
                import traceback
                traceback.print_exc()
                pass

        self._fill_none_field(current_object)

        return self

