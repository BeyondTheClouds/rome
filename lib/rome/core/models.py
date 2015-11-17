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
        # </SOFT DELETE IMPLEMENTATION>

        # # <HARD DELETE IMPLEMENTATION>
        # if session is not None:
        #     session.delete(self)
        #     return
        # database_driver.get_driver().remove_key(self.__tablename__, self.id)
        # # </HARD DELETE IMPLEMENTATION>

    def update(self, values, synchronize_session='evaluate', request_uuid=uuid.uuid1(), do_save=True, skip_session=False):

        primitive = (int, str, bool)

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
        # if not skip_session and getattr(self, "_session", None) is not None:
        #     self._session.add(self)
        # else:
        #     self.save(request_uuid=request_uuid)
        return self

    def save(self, session=None, request_uuid=uuid.uuid1(), force=False, no_nested_save=False, increase_version=True):

        # if getattr(self, "_session", session) is not None:
        #     if not force:
        #         return

        if session is not None:
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

        saving_candidates = object_converter.complex_cache

        if no_nested_save:
            key = object_converter.get_cache_key(self)
            saving_candidates = {
                key: saving_candidates[key]
            }

        for key in [key for key in saving_candidates if "x" in key]:

            classname = "_".join(key.split("_")[0:-1])
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

            classname = "_".join(key.split("_")[0:-1])
            table_name = get_model_tablename_from_classname(classname)

            current_object = object_converter.complex_cache[key]
            target_object = object_converter.target_cache[key]

            current_object["nova_classname"] = table_name

            if not "id" in current_object or current_object["id"] is None:
                current_object["id"] = self.next_key(table_name)
            else:
                model_class = get_model_class_from_name(classname)
                existing_object = database_driver.get_driver().get(table_name, current_object["id"])
                # force_save = force and self.__tablename__ == current_object["nova_classname"] and self.id == current_object["id"]

                # if existing_object is not None:
                #     # WARNING: check if the 0 is correct or if it should be existing_object["rome_version_number"] + 1
                #     version_number = getattr(target_object, "rome_version_number", 0)
                #     # version_number = getattr(target_object, "rome_version_number", existing_object["rome_version_number"])
                #
                #     # version_number = getattr(self, "rome_version_number", existing_object["rome_version_number"])
                #     # version_number = current_object["rome_version_number"] if "rome_version_number" in current_object else 0
                #     # current_object
                #     logging.info("check version: current:%i vs existing:%i (classname:%s, id:%s)" % (version_number, existing_object["rome_version_number"], table_name, current_object["id"]))
                #     if version_number < existing_object["rome_version_number"]:
                #         continue
                if not same_version(existing_object, current_object, model_class):
                    current_object = merge_dict(existing_object, current_object)
                else:
                    continue

            if current_object["id"] == -1:
                logging.debug("skipping the storage of object %s" % (current_object["id"]))
                continue

            object_converter_datetime = get_encoder(request_uuid)

            if (current_object.has_key("created_at") and current_object[
                "created_at"] is None) or not current_object.has_key("created_at"):
                current_object["created_at"] = object_converter_datetime.simplify(datetime.datetime.utcnow())
            current_object["updated_at"] = object_converter_datetime.simplify(datetime.datetime.utcnow())

            logging.debug("starting the storage of %s" % (current_object))

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
