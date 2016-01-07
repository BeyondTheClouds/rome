"""Query module.

This module contains a definition of object queries.

"""

import traceback
import inspect
import re
import logging
import uuid

from lib.rome.core.terms.terms import *
from sqlalchemy.sql.expression import BinaryExpression, BooleanClauseList
import lib.rome.driver.database_driver as database_driver
from lib.rome.core.rows.rows import construct_rows, find_table_name, all_selectable_are_functions

from lib.rome.core.models import get_model_class_from_name, get_model_classname_from_tablename, get_model_tablename_from_classname, get_tablename_from_name

try:
    from lib.rome.core.dataformat import get_decoder
    from lib.rome.core.dataformat.json import find_table_name
except:
    pass
import uuid

class Query:

    def __init__(self, *args, **kwargs):
        self._models = []
        self._initial_models = []
        self._criterions = []
        self._funcs = []
        self._hints = []
        self._session = None
        base_model = None
        if "base_model" in kwargs:
            base_model = kwargs.get("base_model")
        if "session" in kwargs:
            self._session = kwargs.get("session")
        for arg in args:
            if ("count" in str(arg) or "sum" in str(arg)) and "DeclarativeMeta" not in str(type(arg)):
                function_name = re.sub("\(.*\)", "", str(arg))
                field_id = re.sub("\)", "", re.sub(".*\(", "", str(arg)))
                self._models += [Selection(None, None, is_function=True, function=Function(function_name, field_id))]
            elif find_table_name(arg) != "none":
                arg_as_text = "%s" % (arg)
                attribute_name = "*"
                if not hasattr(arg, "_sa_class_manager"):
                    if (len(arg_as_text.split(".")) > 1):
                        attribute_name = arg_as_text.split(".")[-1]
                    if hasattr(arg, "_sa_class_manager"):
                        self._models += [Selection(arg, attribute_name)]
                    elif hasattr(arg, "class_"):
                        self._models += [Selection(arg.class_, attribute_name)]
                else:
                    self._models += [Selection(arg, "*")]
                    pass
            elif isinstance(arg, Selection):
                self._models += [arg]
            elif isinstance(arg, Hint):
                self._hints += [arg]
            elif isinstance(arg, Function):
                self._models += [Selection(None, None, True, arg)]
                self._funcs += [arg]
            elif isinstance(arg, BooleanClauseList) or type(arg) == list:
                for clause in arg:
                    if type(clause) == BinaryExpression:
                        self._criterions += [BooleanExpression("NORMAL", clause)]
            elif isinstance(arg, BinaryExpression):
                self._criterions += [BooleanExpression("NORMAL", arg)]
            elif hasattr(arg, "is_boolean_expression"):
                self._criterions += [arg]
            else:
                pass
        if all_selectable_are_functions(self._models):
            if base_model:
                self._models += [Selection(base_model, "*", is_hidden=True)]

    def all(self, request_uuid=None):
        result_list = construct_rows(self._models, self._criterions, self._hints, session=self._session, request_uuid=request_uuid)
        result = []
        for r in result_list:
            ok = True
            if ok:
                result += [r]
        return result

    def first(self):
        rows = self.all()
        if len(rows) > 0:
            return rows[0]
        else:
            None

    def exists(self):
        return self.first() is not None

    def count(self):
        return len(self.all())

    def soft_delete(self, synchronize_session=False):
        for e in self.all():
            try:
                e.soft_delete()
            except:
                pass
        return self

    def delete(self, synchronize_session=False):
        for e in self.all():
            try:
                e.delete()
            except:
                pass
        return self

    def update(self, values, synchronize_session='evaluate'):
        result = self.all()
        for each in result:
            try:
                each.update(values)
                if self._session is not None:
                    self._session.add(each)
            except:
                pass

    def distinct(self):
        return list(set(self.all()))

    ####################################################################################################################
    # Query construction
    ####################################################################################################################

    def _extract_hint(self, criterion):
        if hasattr(criterion, "extract_hint"):
            self._hints += criterion.extract_hint()
        elif type(criterion).__name__ == "BinaryExpression":
            exp = BooleanExpression("or", *[criterion])
            self._extract_hint(exp)

    def _extract_models(self, criterion):
        tables = []

        """ This means that the current criterion is involving a constant value: there
            is not information that could be collected about a join between tables. """
        if ":" in str(criterion):
            return
        else:
            """ Extract tables names from the criterion. """
            expressions = [criterion.expression.left, criterion.expression.right] if hasattr(criterion, "expression") else []
            for expression in expressions:
                if str(expression) == "NULL":
                    return
                if hasattr(expression, "foreign_keys"):
                    for foreign_key in getattr(expression, "foreign_keys"):
                        if hasattr(foreign_key, "column"):
                            tables += [foreign_key.column.table]
        tables_objects = getattr(criterion, "_from_objects", [])
        tables_names = map(lambda x: str(x), tables_objects)
        tables += tables_names
        tables = list(set(tables)) # remove duplicate names

        """ Extract the missing entity models from tablenames. """
        current_entities = map(lambda x: x._model, self._models)
        current_entities = filter(lambda x: x is not None, current_entities)
        current_entities_tablenames = map(lambda x: x.__tablename__, current_entities)
        missing_tables = filter(lambda x: x not in current_entities_tablenames, tables)
        missing_tables_names = map(lambda x: str(x), missing_tables)
        missing_entities_names = map(lambda x: get_model_classname_from_tablename(x), missing_tables_names)
        missing_entities_objects = map(lambda x: get_model_class_from_name(x), missing_entities_names)

        """ Add the missing entity models to the models of the current query. """
        missing_models_to_selections = map(lambda x: Selection(x, "id", is_hidden=True), missing_entities_objects)
        self._models += missing_models_to_selections

    def filter_by(self, **kwargs):
        criterions = []
        for a in kwargs:
            for selectable in self._models:
                try:
                    column = getattr(selectable._model, a)
                    criterion = column.__eq__(kwargs[a])
                    self._extract_hint(criterion)
                    criterions += [criterion]
                    break
                except Exception as e:
                    # create a binary expression
                    # traceback.print_exc()
                    pass
        return self.filter(*criterions)

    def filter_dict(self, filters):
        return self.filter_by(**filters)

    # criterions can be a function
    def filter(self, *criterions):
        _func = self._funcs[:]
        _criterions = self._criterions[:]
        for criterion in criterions:
            self._extract_hint(criterion)
            self._extract_models(criterion)
            _criterions += [criterion]
        _hints = self._hints[:]
        args = self._models + _func + _criterions + _hints + self._initial_models
        kwargs = {}
        if self._session is not None:
            kwargs["session"] = self._session
        return Query(*args, **kwargs)

    def join(self, *args, **kwargs):
        _func = self._funcs[:]
        _models = self._models[:]
        _criterions = self._criterions[:]
        _hints = self._hints[:]
        for arg in args:

            """ The following block has been written to handle the following kind of call to 'join':

                    query.join("_metadata")

                where the joining class is not specified but rather a relationship name.
            """
            # if type(arg) is str and len(args) == 1:
            #     if len(self._models) == 0:
            #         continue
            #     candidate_model = self._models[0]._model
            #     if not hasattr(candidate_model, arg):
            #         continue
            #     candidate_attribute = getattr(candidate_model, arg)
            #     if not hasattr(candidate_attribute, "property"):
            #         continue
            #     if not type(candidate_attribute.property).__name__ == "RelationshipProperty":
            #         continue
            #     remote_model = candidate_attribute.property.argument
            #     return self.join(remote_model)

            if not isinstance(arg, list) and not isinstance(arg, tuple):
                tuples = [arg]
            else:
                tuples = arg

            for item in tuples:
                is_class = inspect.isclass(item)
                is_expression = (
                    "BinaryExpression" in "%s" % (item) or
                    "BooleanExpression" in "%s" % (item) or
                    "BinaryExpression" in "%s" % (type(item)) or
                    "BooleanExpression" in "%s" % (type(item))
                )
                if is_class:
                    _models = _models + [Selection(item, "*")]
                    if len(tuples) == 1:
                        # Must find an expression that would specify how to join the tables.
                        from lib.rome.core.utils import get_relationships_from_class

                        tablename = item.__tablename__
                        current_tablenames = map(lambda x: x._model.__tablename__, _models)
                        models_classes = map(lambda x: x._model, _models)
                        relationships = map(lambda x: get_relationships_from_class(x), models_classes)
                        # relationships = get_relationships_from_class(item)
                        flatten_relationships = [item for sublist in relationships for item in sublist]
                        for relationship in flatten_relationships:
                            tablesnames = [relationship.local_tablename, relationship.remote_object_tablename]
                            if tablename in tablesnames:
                                other_tablename = filter(lambda x: x!= tablename, tablesnames)[0]
                                if other_tablename in current_tablenames:
                                    type_expression = type(relationship.initial_expression).__name__
                                    new_criterions = []
                                    if type_expression == "BooleanClauseList":
                                        for exp in relationship.initial_expression:
                                            new_criterions += [JoiningBooleanExpression("NORMAL", *[exp])]
                                    elif type_expression == "BinaryExpression":
                                        new_criterions = [JoiningBooleanExpression("NORMAL", *[relationship.initial_expression])]
                                    _criterions += new_criterions
                                    break
                elif is_expression:
                    _criterions += [item]
                else:
                    pass
        args = _models + _func + _criterions + _hints + self._initial_models
        kwargs = {}
        if self._session is not None:
            kwargs["session"] = self._session
        return Query(*args, **kwargs)

    def outerjoin(self, *args, **kwargs):
        return self.join(*args, **kwargs)

    def options(self, *args):
        _func = self._funcs[:]
        _models = self._models[:]
        _criterions = self._criterions[:]
        _initial_models = self._initial_models[:]
        _hints = self._hints[:]
        args = _models + _func + _criterions + _hints + _initial_models
        kwargs = {}
        if self._session is not None:
            kwargs["session"] = self._session
        return Query(*args, **kwargs)

    def order_by(self, *criterion):
        _func = self._funcs[:]
        _models = self._models[:]
        _criterions = self._criterions[:]
        _initial_models = self._initial_models[:]
        _hints = self._hints[:]
        args = _models + _func + _criterions + _hints + _initial_models
        kwargs = {}
        if self._session is not None:
            kwargs["session"] = self._session
        return Query(*args, **kwargs)

    def with_lockmode(self, mode):
        return self

    def subquery(self):
        _func = self._funcs[:]
        _models = self._models[:]
        _criterions = self._criterions[:]
        _initial_models = self._initial_models[:]
        _hints = self._hints[:]
        args = _models + _func + _criterions + _hints + _initial_models
        kwargs = {}
        if self._session is not None:
            kwargs["session"] = self._session
        return Query(*args, **kwargs).all()

    def __iter__(self):
        return iter(self.all())

    def __repr__(self):
        return """{\\"models\\": \\"%s\\", \\"criterions\\": \\"%s\\", \\"hints\\": \\"%s\\"}""" % (self._models, self._criterions, self._hints)
