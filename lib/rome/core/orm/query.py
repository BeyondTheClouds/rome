"""Query module.

This module contains a definition of object queries.

"""

import traceback
import inspect
import re
import logging

from lib.rome.core.terms.terms import *
from sqlalchemy.sql.expression import BinaryExpression
import lib.rome.driver.database_driver as database_driver
from lib.rome.core.rows.rows import construct_rows, find_table_name, all_selectable_are_functions

try:
    from lib.rome.core.dataformat.deconverter import JsonDeconverter
    from lib.rome.core.dataformat.deconverter import find_table_name
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
            elif isinstance(arg, BinaryExpression):
                self._criterions += [BooleanExpression("NORMAL", arg)]
            elif hasattr(arg, "is_boolean_expression"):
                self._criterions += [arg]
            else:
                pass
        if all_selectable_are_functions(self._models):
            if base_model:
                self._models += [Selection(base_model, "*", is_hidden=True)]

    def all(self):
        result_list = construct_rows(self._models, self._criterions, self._hints, session=self._session)
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
        return self

    def update(self, values, synchronize_session='evaluate'):
        try:
            from lib.rome.core.dataformat.deconverter import JsonDeconverter
        except:
            pass
        rows = self.all()
        for row in rows:
            tablename = find_table_name(row)
            id = row.id
            logging.debug("may need to update %s@%s with %s" % (str(id), tablename, values))
            data = database_driver.get_driver().get(tablename, id)
            for key in values:
                data[key] = values[key]
            request_uuid = uuid.uuid1()
            object_desimplifier = JsonDeconverter(request_uuid=request_uuid)
            try:
                desimplified_object = object_desimplifier.desimplify(data)
                desimplified_object.save()
            except Exception as e:
                traceback.print_exc()
                logging.error("could not save %s@%s" % (str(id), tablename))
                return None
        return len(rows)

    def distinct(self):
        return list(set(self.all()))

    ####################################################################################################################
    # Query construction
    ####################################################################################################################

    def _extract_hint(self, criterion):
        try:
            if hasattr(criterion.expression.right, "value"):
                table_name = str(criterion.expression.left.table)
                attribute_name = str(criterion.expression.left.key)
                value = "%s" % (criterion.expression.right.value)
                self._hints += [Hint(table_name, attribute_name, value)]
        except:
            pass

    def filter_by(self, **kwargs):
        _func = self._funcs[:]
        _criterions = self._criterions[:]
        for a in kwargs:
            for selectable in self._models:
                try:
                    column = getattr(selectable._model, a)
                    criterion = column.__eq__(kwargs[a])
                    self._extract_hint(criterion)
                    _criterions += [criterion]
                    break
                except Exception as e:
                    # create a binary expression
                    # traceback.print_exc()
                    pass
        _hints = self._hints[:]
        args = self._models + _func + _criterions + _hints + self._initial_models
        kwargs = {}
        if self._session is not None:
            kwargs["session"] = self._session
        return Query(*args, **kwargs)

    def filter_dict(self, filters):
        return self.filter_by(**filters)

    # criterions can be a function
    def filter(self, *criterions):
        _func = self._funcs[:]
        _criterions = self._criterions[:]
        for criterion in criterions:
            self._extract_hint(criterion)
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
