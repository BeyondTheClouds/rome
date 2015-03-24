"""Utils module.

This module contains functions, classes and mix-in that are used for the
discovery database backend.

"""

import traceback
import uuid
import datetime as timeutils
import logging
from sqlalchemy import Column, Integer
from sqlalchemy import DateTime

import lib.rome.driver.database_driver as database_driver

def merge_dicts(dict1, dict2):
    """Merge two dictionnaries into one dictionnary: the values containeds
    inside dict2 will erase values of dict1."""
    return dict(dict1.items() + dict2.items())


def find_table_name(model):
    """This function returns the name of the given model as a String. If the
    model cannot be identified, it returns "none".
    :param model: a model object candidate
    :return: the table name or "none" if the object cannot be identified
    """

    if hasattr(model, "__tablename__"):
        return model.__tablename__

    if hasattr(model, "table"):
        return model.table.name

    if hasattr(model, "class_"):
        return model.class_.__tablename__

    if hasattr(model, "clauses"):
        for clause in model.clauses:
            return find_table_name(clause)

    return "none"


def is_lazyreference(obj):
    """Check if the given object is a lazy reference to an instance of a
    NovaBase."""

    value = str(obj)
    return value.startswith("Lazy(") and value.endswith(")")


def is_novabase(obj):
    """Check if the given object is an instance of a NovaBase."""

    try:
        found_table_name = find_table_name(obj.__class__) is not "none"
        is_lazy = is_lazyreference(obj)
        return found_table_name or is_lazy
    except:
        pass

    return False


def get_single_object(tablename, id, desimplify=True, request_uuid=None, skip_loading=False):
    from lib.rome.core.dataformat.deconverter import JsonDeconverter

    if isinstance(id, int):
        object_deconverter = JsonDeconverter(request_uuid=request_uuid)
        data = database_driver.get_driver().get(tablename, id)
        if desimplify:
            try:
                model_object = object_deconverter.desimplify(data)
                if not skip_loading:
                    model_object.load(data=data)
                return model_object
            except Exception as e:
                traceback.print_exc()
                return None
        else:
            return data
    else:
        return None


def get_objects(tablename, desimplify=True, request_uuid=None, skip_loading=False):
    from lib.rome.core.dataformat.deconverter import JsonDeconverter

    object_deconverter = JsonDeconverter(request_uuid=request_uuid)

    def transform(data):
        model_object = object_deconverter.desimplify(data)
        if not skip_loading:
            model_object.load(data=data)
        return model_object

    data = database_driver.get_driver().getall(tablename)
    result = map(lambda x: transform(x), data)

    return result


def get_models_satisfying(tablename, field, value, request_uuid=None):
    candidates = get_objects(tablename, False, request_uuid=request_uuid)
    result = []
    for each in candidates:
        if each[field] == value:
            result += [each]
    return result


class RelationshipModel(object):
    """Class that will ease the representation of relationships: a can
    be represented either through a foreign key value or a foreign
    object."""

    def __init__(self, local_fk_field, local_fk_value, local_object_field, local_object_value, remote_object_field,
                 remote_object_tablename, is_list):
        """Constructor"""

        self.local_fk_field = local_fk_field
        self.local_object_field = local_object_field
        self.remote_object_field = remote_object_field
        self.local_fk_value = local_fk_value
        self.local_object_value = local_object_value
        self.remote_object_tablename = remote_object_tablename
        self.is_list = is_list

    def __unicode__(self):
        return "{local_fk_field: %s, local_fk_value: %s} <--> {local_object_field:%s, remote_object_field:%s, local_object_value:%s, remote_object_tablename:%s, is_list:%s}" % (
            self.local_fk_field,
            self.local_fk_value,
            self.local_object_field,
            self.remote_object_field,
            self.local_object_value,
            self.remote_object_tablename,
            self.is_list
        )

    def __str__(self):
        return self.__unicode__()

    def soft_delete(self, session):
        """Mark this object as deleted."""
        self.deleted = self.id
        self.deleted_at = timeutils.utcnow()
        self.save(session=session)

class ModelBase(object):
    def get(self, key, default=None):
        pass
    def save(self, session):
        pass
    def update(self, values):
        pass

class TimestampMixin(object):
    created_at = Column(DateTime, default=lambda: timeutils.utcnow())
    updated_at = Column(DateTime, onupdate=lambda: timeutils.utcnow())

class SoftDeleteMixin(object):
    deleted_at = Column(DateTime)
    deleted = Column(Integer, default=0)

class ReloadableRelationMixin(TimestampMixin, SoftDeleteMixin, ModelBase):
    """Mixin that contains several methods that will be in charge of enabling
    NovaBase instances to reload default values and relationships."""

    def reload_default_values(self):
        """Reload the default values of un-setted fields that."""

        for field in self._sa_class_manager:
            state = self._sa_instance_state
            field_value = getattr(self, field)
            if field_value is None:
                try:
                    field_column = state.mapper._props[field].columns[0]
                    field_name = field_column.name
                    field_default_value = field_column.default.arg
                    if not "function" in str(type(field_default_value)):
                        setattr(field_name, field_default_value)
                except:
                    pass

    def get_relationships(obj):
        result = []

        state = obj._sa_instance_state

        for field in obj._sa_class_manager:
            field_object = obj._sa_class_manager[field]
            field_column = state.mapper._props[field]

            contain_comparator = hasattr(field_object, "comparator")
            is_relationship = ("relationship" in str(field_object.comparator)
                               if contain_comparator else False
                               )
            if is_relationship:
                remote_local_pair = field_object.property.local_remote_pairs[0]

                local_fk_field = remote_local_pair[0].name
                local_fk_value = getattr(obj, local_fk_field)
                local_object_field = field
                local_object_value = getattr(obj, local_object_field)
                remote_object_field = remote_local_pair[1].name
                remote_object_tablename = str(remote_local_pair[1].table)
                is_list = field_object.property.uselist

                result += [RelationshipModel(
                    local_fk_field,
                    local_fk_value,
                    local_object_field,
                    local_object_value,
                    remote_object_field,
                    remote_object_tablename,
                    is_list
                )]

        return result

    def update_foreign_keys(self, request_uuid=uuid.uuid1()):
        """Update foreign keys according to local fields' values."""

        from lazy_reference import LazyReference

        if hasattr(self, "metadata"):
            metadata = self.metadata
            tablename = self.__tablename__

            if metadata and tablename in metadata.tables:
                for fk in metadata.tables[tablename].foreign_keys:
                    local_field_name = str(fk.parent).split(".")[-1]
                    remote_table_name = fk._colspec.split(".")[-2]
                    remote_field_name = fk._colspec.split(".")[-1]

                    if hasattr(self, remote_table_name):
                        pass
                    else:
                        """Remove the "s" at the end of the tablename"""
                        remote_table_name = remote_table_name[:-1]
                        pass

                    try:
                        remote_object = getattr(self, remote_table_name)
                        remote_field_value = getattr(
                            remote_object,
                            remote_field_name
                        )
                        setattr(self, local_field_name, remote_field_value)
                    except Exception as e:
                        pass
        try:
            from lib.rome.core.dataformat.deconverter import JsonDeconverter
        except:
            pass

        object_deconverter = JsonDeconverter(request_uuid=request_uuid)
        for each in self.get_relationships():
            if each.local_fk_value is None and each.local_object_value is None:
                continue

            if not each.local_fk_value is None:
                if each.remote_object_field is "id":

                    remote_ref = LazyReference(
                        each.remote_object_tablename,
                        each.local_fk_value,
                        request_uuid,
                        object_deconverter
                    )
                    setattr(self, each.local_object_field, remote_ref)
                else:
                    # dirty fix (grid'5000 debugging)
                    if self.__tablename__ == "services":
                        pass
                    else:
                        continue
                    candidates = get_models_satisfying(
                        each.remote_object_tablename,
                        each.remote_object_field,
                        each.local_fk_value,
                        request_uuid=request_uuid
                    )

                    lazy_candidates = []
                    for cand in candidates:
                        ref = LazyReference(
                            cand["nova_classname"],
                            cand["id"],
                            request_uuid,
                            object_deconverter
                        )
                        lazy_candidates += [ref]
                    if not each.is_list:
                        if len(lazy_candidates) is 0:
                            logging.error(("could not find an accurate candidate"
                               " for (%s, %s) in %s") % (
                                  each.remote_object_tablename,
                                  each.remote_object_field,
                                  each.local_fk_value
                              ))
                        else:
                            setattr(
                                self,
                                each.local_object_field,
                                lazy_candidates[0]
                            )
                            pass
                    else:
                        setattr(
                            self,
                            each.local_object_field,
                            lazy_candidates
                        )
                        # for cand in lazy_candidates:
                        # setattr(
                        #         cand,
                        #         each.remote_object_field,
                        #         each.local_fk_value
                        #     )
                        #     pass

                        # print("   * %s@%s -> rel(%s)" % (str(self.id), str(self.__tablename__), each))
                        pass

