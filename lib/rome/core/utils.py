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
import time
import math
import time

# from oslo.utils import timeutils
try:
    from oslo.utils import timeutils
except:
    from oslo_utils import timeutils

# from oslo.db.exception import DBDeadlock
try:
    from oslo.db.exception import DBDeadlock
except:
    from oslo_db.exception import DBDeadlock

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def datetime_to_int(x):
    return int(x.strftime('%s'))

current_milli_time = lambda: int(round(time.time() * 1000))

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
    if hasattr(obj, "is_relationship_list"):
        return False
    if hasattr(obj, "__tablename__") or hasattr(obj, "lazy_backref_buffer"):
        return True
    return False


def get_single_object(tablename, id, desimplify=True, request_uuid=None, skip_loading=False):
    from lib.rome.core.dataformat import get_decoder

    if isinstance(id, int):
        object_deconverter = get_decoder(request_uuid=request_uuid)
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


def transform(data, deconverter, skip_loading):
    result = deconverter.desimplify(data)
    return result


# from lib.rome.utils.LimitedSizeDictionnary import LimitedSizeDictionnary
# DB_CACHES = LimitedSizeDictionnary(size_limit=50)
#
# def get_objects(tablename, desimplify=True, request_uuid=None, skip_loading=False, hints=[]):
#     if request_uuid is not None:
#         db_cache_key = "%s_%s_%s" % (tablename, request_uuid, hints)
#         if db_cache_key in DB_CACHES:
#             return DB_CACHES[db_cache_key]
#     data = database_driver.get_driver().getall(tablename, hints=hints)
#     if request_uuid is not None:
#         DB_CACHES[db_cache_key] = data
#     return data

# DB_CACHES = LimitedSizeDictionnary(size_limit=50)
DB_CACHES = {}
cache_time_limit = 100


def current_milli_time():
    return int(round(time.time() * 200))


def get_objects(tablename, desimplify=True, request_uuid=None, skip_loading=False, hints=[]):
    return database_driver.get_driver().getall(tablename, hints=hints)


def get_models_satisfying(tablename, field, value, request_uuid=None, hints=[]):
    candidates = get_objects(tablename, False, request_uuid=request_uuid, hints=hints)
    result = []
    for each in candidates:
        if each[field] == value:
            result += [each]
    return result


class RelationshipModel(object):
    """Class that will ease the representation of relationships: a can
    be represented either through a foreign key value or a foreign
    object."""

    def __init__(self, local_fk_field, local_fk_value, local_object_field, local_object_value, local_object_type, local_tablename, remote_object_field,
                 remote_object_tablename, is_list, remote_class=None, expression=None, initial_expression=None, to_many=False, obj=None,
                 direction=""):
        """Constructor"""

        self.local_fk_field = local_fk_field
        self.local_object_field = local_object_field
        self.remote_object_field = remote_object_field
        self.local_fk_value = local_fk_value
        self.local_object_value = local_object_value
        self.local_object_type = local_object_type
        self.local_tablename = local_tablename
        self.remote_object_tablename = remote_object_tablename
        self.is_list = is_list

        self.remote_class = remote_class
        self.expression = expression
        self.initial_expression = initial_expression
        self.to_many = to_many
        self.obj = obj
        self.direction = direction
        self._convert_local_object_value()

    def given_type_is_subtype(self, given_type, type_candidates):
        for type_candidate in type_candidates:
            if str.upper(type_candidate) in str.upper(given_type):
                return True
        return False

    def _convert_local_object_value(self):
        initial_value = self.local_fk_value
        value = initial_value
        value_changed = False
        fk_typename = type(self.local_object_type).__name__
        if value is None:
            return
        value_typename = type(self.local_object_type).__name__
        # if fk_typename in ["Integer", "Float"]:
        if self.given_type_is_subtype(fk_typename, ["Integer", "Float"]):
            if value_typename == "str" and "." in value:
                value = float(value)
                value_changed = True
            if type(value) is float and math.isnan(value):
                    value = 0
            if self.given_type_is_subtype(fk_typename, ["Integer"]):
                if hasattr(value, self.remote_object_field):
                    value = getattr(value, self.remote_object_field)
                value = int(value)
                value_changed = True
        if value_changed:
            self.local_fk_value = value

    def __repr__(self):
        return "{local_fk_field: %s, local_fk_value: %s} <--> {local_object_field:%s, remote_object_field:%s, local_object_value:%s, remote_object_tablename:%s, is_list:%s}" % (
            self.local_fk_field,
            self.local_fk_value,
            self.local_object_field,
            self.remote_object_field,
            self.local_object_value,
            self.remote_object_tablename,
            self.is_list
        )

    def soft_delete(self, session):
        """Mark this object as deleted."""
        self.deleted = self.id
        self.deleted_at = timeutils.utcnow()
        self.save(session=session)

class ModelBase(object):
    def get(self, key, default=None):
        pass

    def save(self, session, force=True):
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

    def get_relationships(self, foreignkey_mode=False):
        return get_relationships(self, foreignkey_mode=foreignkey_mode)

    def get_relationship_fields(self, foreignkey_mode=False, with_indirect_field=True):
        relationships = get_relationships(self, foreignkey_mode=foreignkey_mode)
        results = map(lambda x: x.local_object_field, relationships)
        if with_indirect_field:
            # many_to_one_relationships = filter(lambda x: "MANYTOONE" in x.direction, relationships)
            many_to_one_relationships = relationships
            results += map(lambda x: x.local_fk_field, many_to_one_relationships)
        results = filter(lambda x: x not in ["id"],  results)
        return list(set(results))

    def update_foreign_keys(self, request_uuid=uuid.uuid1()):
        """Update foreign keys according to local fields' values."""
        return

    def load_relationships(self, filter_keys=[], request_uuid=uuid.uuid1()):
        """Update foreign keys according to local fields' values."""
        # for rel in self.get_relationships(foreignkey_mode=True):
        #     self.__dict__[rel.local_object_field] = LazyRelationship(rel)
        attrs = self.__dict__
        for rel in self.get_relationships(foreignkey_mode=True):
            key = rel.local_object_field
            if len(filter_keys) > 0 and key not in filter_keys:
                continue
            if key in attrs and attrs[key] is not None:
                continue
            attrs[key] = LazyRelationship(rel)
        pass

    def unload_relationships(self, request_uuid=uuid.uuid1()):
        """Update foreign keys according to local fields' values."""
        # for rel in self.get_relationships():
        #     if rel.is_list:
        #         self.__dict__[rel.local_object_field] = []
        #     else:
        #         self.__dict__[rel.local_object_field] = None
        #         pass
        pass

    # def get_relationship_fields(self):
    #     return map(lambda x:x.local_object_field, self.get_relationships())

CACHING_FAKE_INSTANCES = {}

def get_fake_instance_relationships(classname):
    from lib.rome.core.models import get_model_class_from_name
    if not classname in CACHING_FAKE_INSTANCES:
        fake_instance = get_model_class_from_name(classname)().get_relationships()
        CACHING_FAKE_INSTANCES[classname] = fake_instance
    return CACHING_FAKE_INSTANCES[classname]

def get_relationships_from_class(cls, foreignkey_mode=False):
    from sqlalchemy.sql.expression import BinaryExpression, BooleanClauseList, BindParameter

    # state = obj._sa_instance_state

    result = []

    fields = {}
    for key in cls._sa_class_manager:
        fields[key] = cls._sa_class_manager[key]

    for field in fields:
        field_object = fields[field]

        contain_comparator = hasattr(field_object, "comparator")
        is_relationship = ("relationship" in str(field_object.comparator)
                           if contain_comparator else False
                           )
        if is_relationship:
            remote_local_pair = field_object.property.local_remote_pairs[0]

            local_fk_field = remote_local_pair[0].name
            local_fk_value = None
            local_tablename = cls.__tablename__
            local_object_field = field
            local_object_value = None
            local_object_type = None
            try:
                local_object_type = remote_local_pair[0].type
            except:
                pass
            remote_object_field = remote_local_pair[1].name
            remote_object_tablename = str(remote_local_pair[1].table)
            is_list = field_object.property.uselist

            remote_class = cls
            expression = field_object.expression
            direction = str(field_object.property.direction).split("'")[1]

            if type(expression) == BinaryExpression:
                expression = [expression]

            to_many = is_list

        if is_relationship:
            result += [RelationshipModel(
                local_fk_field,
                local_fk_value,
                local_object_field,
                local_object_value,
                local_object_type,
                local_tablename,
                remote_object_field,
                remote_object_tablename,
                is_list,
                remote_class=remote_class,
                expression=expression,
                initial_expression=expression,
                to_many=to_many,
                obj=None,
                direction=direction
            )]
    return result

def get_relationships(obj, foreignkey_mode=False):
    from sqlalchemy.sql.expression import BinaryExpression, BooleanClauseList, BindParameter

    def filter_matching_column(clause, tablename):
        # return clause
        if tablename in str(clause.left):
            value = getattr(obj, clause.left.description)
            if value is None and clause.left.default is not None:
                value = clause.left.default.arg
            clause.left = BindParameter(key="totoo", value=value)
        if tablename in str(clause.right):
            value = getattr(obj, clause.right.description)
            if value is None and clause.right.default is not None:
                value = clause.right.default.arg
            clause.right = BindParameter(key="totoo", value=value)
        return clause

    import models

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
            local_tablename = obj.__tablename__
            local_object_field = field
            local_object_type = None
            try:
                local_object_type = remote_local_pair[0].type
            except:
                pass
            local_object_value = getattr(obj, local_object_field)
            remote_object_field = remote_local_pair[1].name
            remote_object_tablename = str(remote_local_pair[1].table)
            is_list = field_object.property.uselist

            local_table_name = obj.__tablename__

            remote_class = models.get_model_class_from_name(models.get_model_classname_from_tablename(remote_object_tablename))
            expression = field_object.property.primaryjoin
            initial_expression=expression
            direction = str(field_object.property.direction).split("'")[1]

            if type(expression) == BinaryExpression:
                expression = [expression]

            if foreignkey_mode:
                corrected_expression = map(lambda  x: filter_matching_column(x, local_table_name), expression)
                expression = corrected_expression

            # to_many="TOMANY" in str(field_object.property.direction)
            to_many = is_list

            result += [RelationshipModel(
                local_fk_field,
                local_fk_value,
                local_object_field,
                local_object_value,
                local_object_type,
                local_tablename,
                remote_object_field,
                remote_object_tablename,
                is_list,
                remote_class=remote_class,
                expression=expression,
                initial_expression=initial_expression,
                to_many=to_many,
                obj=obj,
                direction=direction
            )]
    return result


class LazyRelationship():
    def __init__(self, rel, request_uuid=None):
        from lib.rome.core.orm.query import Query
        self.data = None
        self.rel = rel
        self.request_uuid = request_uuid
        self.is_loaded = False
        self.is_relationship_list = self.rel.to_many
        # print(self.request_uuid)
        # self.query = Query(rel.remote_class)
        # self.query = self.query.filter(getattr(rel.remote_class, rel.remote_object_field)==rel.local_fk_value)

    def reload(self):
        def match(x, rel):
            field_name = rel.remote_object_field
            x_value = getattr(x, field_name, "None")
            return x_value == rel.local_fk_value
        if self.data is not None:
            return
        data = database_driver.get_driver().getall(self.rel.remote_object_tablename, [[self.rel.remote_object_field, self.rel.local_fk_value]])

        if len(data) == 0:
            from lib.rome.core.orm.query import Query
            self.query = Query(self.rel.remote_class)
            self.query = self.query.filter(getattr(self.rel.remote_class, self.rel.remote_object_field)==self.rel.local_fk_value)
            if self.request_uuid:
                data = self.query.all(request_uuid=self.request_uuid) #if self.rel.to_many else self.query.first()data
            else:
                data = self.query.all() #if self.rel.to_many else self.query.first()data
        else:
            from lib.rome.core.lazy import LazyValue
            data = map(lambda x: LazyValue(x, self.request_uuid), data)
        self.__dict__["data"] = data
        self.data = filter(lambda x: match(x, self.rel), self.data)
        if not self.rel.to_many:
            if len(self.data) > 0:
                self.data = self.data[0]
            else:
                self.data = None
        self.is_loaded = True

    def __getattr__(self, item):
        if item not in ["data", "rel", "query", "is_relationship_list", "is_loaded", "request_uuid"]:
            self.reload()
        if item == "iteritems":
            if self.is_relationship_list:
                return self.data.iteritems
            else:
                None
        if item == "__nonzero__" and self.is_relationship_list:
            return getattr(self.data, "__len__", None)
        return getattr(self.data, item, None)

    def __setattr__(self, name, value):
        if name in ["data", "rel", "query", "is_relationship_list", "is_loaded", "request_uuid"]:
            self.__dict__[name] = value
        else:
            self.reload()
            setattr(self.data, name, value)
            return self
