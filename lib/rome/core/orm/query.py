"""Query module.

This module contains a definition of object queries.

"""

import datetime
import itertools
import traceback
import inspect
import re
import logging


from sqlalchemy.util._collections import KeyedTuple
from sqlalchemy.sql.expression import BinaryExpression
import pytz

import lib.rome.core.utils as utils
import lib.rome.driver.database_driver as database_driver

try:
    from lib.rome.core.dataformat.deconverter import JsonDeconverter
    from lib.rome.core.dataformat.deconverter import find_table_name
except:
    pass
import uuid


file_logger_enabled = False
try:
    file_logger = logging.getLogger('rome_file_logger')
    hdlr = logging.FileHandler('/opt/logs/rome.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    file_logger.addHandler(hdlr)
    file_logger.setLevel(logging.INFO)
    file_logger_enabled = True
except:
    pass

class Selection:
    def __init__(self, model, attributes, is_function=False, function=None, is_hidden=False):
        self._model = model
        self._attributes = attributes
        self._function = function
        self._is_function = is_function
        self.is_hidden = is_hidden

    def __repr__(self):
        return "Selection(%s.%s)" % (self._model, self._attributes)


def and_(*exps):
    return BooleanExpression("AND", *exps)


def or_(*exps):
    return BooleanExpression("OR", *exps)

def has_attribute(obj, key):
    if type(obj) is dict:
        return key in obj
    else:
        return hasattr(obj, key)

def get_attribute(obj, key):
    if type(obj) is dict:
        return obj[key]
    else:
        return getattr(obj, key)

class BooleanExpression(object):
    def __init__(self, operator, *exps):
        self.operator = operator
        self.exps = exps

    def is_boolean_expression(self):
        return True

    def evaluate_criterion(self, criterion, value):

        def uncapitalize(s):
            return s[:1].lower() + s[1:] if s else ''

        def getattr_rec(obj, attr, otherwise=None):
            """ A reccursive getattr function.

            :param obj: the object that will be use to perform the search
            :param attr: the searched attribute
            :param otherwise: value returned in case attr was not found
            :return:
            """
            try:
                if not "." in attr:
                    return get_attribute(obj, attr.replace("\"", ""))
                else:
                    current_key = attr[:attr.index(".")]
                    next_key = attr[attr.index(".") + 1:]
                    if has_attribute(obj, current_key):
                        current_object = get_attribute(obj, current_key)
                    elif has_attribute(obj, current_key.capitalize()):
                        current_object = get_attribute(obj, current_key.capitalize())
                    elif has_attribute(obj, uncapitalize(current_key)):
                        current_object = get_attribute(obj, uncapitalize(current_key))
                    else:
                        current_object = get_attribute(obj, current_key)

                    return getattr_rec(current_object, next_key, otherwise)
            except AttributeError:
                return otherwise

        criterion_str = criterion.__str__()

        if "=" in criterion_str:
            def comparator(a, b):
                if a is None or b is None:
                    return False
                return "%s" % (a) == "%s" % (b) or a == b

            op = "="

        if "REGEXP" in criterion_str:
            def comparator(a, b):
                if a is None or b is None:
                    return False
                return "%s" % (a) == "%s" % (b) or a == b

            op = "REGEXP"

        if "IS" in criterion_str:
            def comparator(a, b):
                if a is None or b is None:
                    if a is None and b is None:
                        return True
                    else:
                        return False
                return a is b

            op = "IS"

        if "!=" in criterion_str:
            def comparator(a, b):
                if a is None or b is None:
                    return False
                return a is not b

            op = "!="

        if "<" in criterion_str:
            def comparator(a, b):
                if a is None or b is None:
                    return False
                return a < b

            op = "<"

        if ">" in criterion_str:
            def comparator(a, b):
                if a is None or b is None:
                    return False
                return a > b

            op = ">"

        if "IN" in criterion_str:
            def comparator(a, b):
                if a is None or b is None:
                    return False
                return a == b or a is b

            op = "IN"

        split = criterion_str.split(op)
        left = split[0].strip()
        right = split[1].strip()
        left_values = []

        # Computing left value
        if left.startswith(":"):
            left_values += [criterion._orig[0].effective_value]
        else:
            left_values += [getattr_rec(value, left.capitalize())]


        # Computing right value
        if right.startswith(":"):
            right_value = criterion._orig[1].effective_value
        else:
            if hasattr(criterion, "_orig"):
                if isinstance(criterion._orig[1], bool):
                    right_value = criterion._orig[1]
                else:
                    right_type_name = "none"
                    try:
                        right_type_name = str(criterion._orig[1].type)
                    except:
                        pass

                    if right_type_name == "BOOLEAN":
                        right_value = right
                        if right_value == "1":
                            right_value = True
                        else:
                            right_value = False
                    else:
                        right_value = getattr_rec(value, right.capitalize())
            elif hasattr(criterion, "is_boolean_expression") and criterion.is_boolean_expression():
                right_value = criterion.evaluate(value)
        # try:
        # print(">>> (%s)[%s] = %s <-> %s" % (value.keys(), left, left_values, right))
        # except:
        #     pass

        result = False
        for left_value in left_values:

            if isinstance(left_value, datetime.datetime):
                if left_value.tzinfo is None:
                    left_value = pytz.utc.localize(left_value)

            if isinstance(right_value, datetime.datetime):
                if right_value.tzinfo is None:
                    right_value = pytz.utc.localize(right_value)

            if "NOT NULL" in right:
                if left_value is not None:
                    result = True
            else:
                if comparator(left_value, right_value):
                    result = True

        if op == "IN":
            result = False
            right_terms = set(criterion.right.element)
            # print("before %s" % (right_terms))

            if left_value is None and hasattr(value, "__iter__"):
                left_key = left.split(".")[-1]
                if value[0].has_key(left_key):
                    left_value = value[0][left_key]

            for right_term in right_terms:
                try:
                    right_value = getattr(right_term.value, "%s" % (right_term._orig_key))
                except AttributeError:
                    right_value = right_term.value

                if isinstance(left_value, datetime.datetime):
                    if left_value.tzinfo is None:
                        left_value = pytz.utc.localize(left_value)

                if isinstance(right_value, datetime.datetime):
                    if right_value.tzinfo is None:
                        right_value = pytz.utc.localize(right_value)
                # print("comparing %s with %s" % (left_value, right_value))
                if comparator(left_value, right_value):
                    result = True
        return result

    def evaluate(self, value):

        if self.operator == "AND":
            if len(self.exps) <= 0:
                return False
            for exp in self.exps:
                if hasattr(exp, "evaluate") and not exp.evaluate(value):
                    return False
                else:
                    if not self.evaluate_criterion(exp, value):
                        return False
            return True

        if self.operator == "OR" or self.operator == "NORMAL":
            for exp in self.exps:
                if hasattr(exp, "evaluate") and exp.evaluate(value):
                    return True
                else:
                    if self.evaluate_criterion(exp, value):
                        return True
            return False

        return True

class Function:
    def __init__(self, name, field):
        self._name = name
        if name == "count":
            self._function = self.count
        elif name == "sum":
            self._function = self.sum
        else:
            self._function = self.sum
        self._field = field

    def collect_field(self, rows, field):
        if rows is None:
            rows = []
        if "." in field:
            fieldtable = field.split(".")[-2]
            fieldname = field.split(".")[-1]
        filtered_rows = []
        for row in rows:
            try:
                iter(row)
            except:
                row = [row]
            for subrow in row:
                if subrow.__tablename__ == fieldtable:
                    filtered_rows += [subrow]
        result = [getattr(row, fieldname) for row in filtered_rows]
        return result

    def count(self, rows):
        collected_field_values = self.collect_field(rows, self._field)
        return len(collected_field_values)

    def sum(self, rows):
        result = 0
        collected_field_values = self.collect_field(rows, self._field)
        try:
            result = sum(collected_field_values)
        except:
            pass
        return result

class Hint():

    def __init__(self, table_name, attribute, value):
        self.table_name = table_name
        self.attribute = attribute
        self.value = value

    def __repr__(self):
        return "%s.%s = %s" % (self.table_name, self.attribute, str(self.value))

def extract_models(l):
    already_processed = set()
    result = []
    for selectable in [x for x in l if not x._is_function]:
        if not selectable._model in already_processed:
            already_processed.add(selectable._model)
            result += [selectable]
    return result


class Query:
    _funcs = []
    _initial_models = []
    _models = []
    _criterions = []
    _hints = []

    def all_selectable_are_functions(self):
        return all(x._is_function for x in [y for y in self._models if not y.is_hidden])

    def __init__(self, *args, **kwargs):
        self._models = []
        self._criterions = []
        self._funcs = []
        self._hints = []

        base_model = None
        if kwargs.has_key("base_model"):
            base_model = kwargs.get("base_model")
        for arg in args:
            if "count" in str(arg) or "sum" in str(arg):
                function_name = re.sub("\(.*\)", "", str(arg))
                field_id = re.sub("\)", "", re.sub(".*\(", "", str(arg)))
                self._models += [Selection(None, None, is_function=True, function=Function(function_name, field_id))]
            elif self.find_table_name(arg) != "none":
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

        if self.all_selectable_are_functions():
            if base_model:
                self._models += [Selection(base_model, "*", is_hidden=True)]

    def find_table_name(self, model):

        """This function return the name of the given model as a String. If the
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
                return self.find_table_name(clause)

        return "none"

    def construct_rows(self):

        """This function constructs the rows that corresponds to the current orm.
        :return: a list of row, according to sqlalchemy expectation
        """

        def building_tuples(list_results, labels):
            mode = "not_cartesian_product"
            if mode is "cartesian_product":
                cartesian_product = []
                for element in itertools.product(*list_results):
                    cartesian_product += [element]
                return cartesian_product
            else:
                # construct dicts that will keep a ref on objects according to their "id" and "uuid" fields.
                indexed_results = {}
                for i in zip(list_results, labels):
                    (results, label) = i
                    dict_result = {"id": {}, "uuid": {}}
                    for j in results:
                        if hasattr(j, "id"):
                            dict_result["id"][j.id] = j
                        if hasattr(j, "uuid"):
                            dict_result["uuid"][j.uuid] = j
                    indexed_results[label] = dict_result
                # find iteratively pairs that matches according to relationship modelisation
                tuples = []
                tuples_labels = []

                # initialise tuples
                count = 0
                for i in zip(list_results, labels):
                    (results, label) = i
                    tuples_labels += [label]
                    for j in results:
                        current_tuple = {label: j}
                        tuples += [current_tuple]
                    break

                # increase model of exisintg tuples
                count == 0
                for i in zip(list_results, labels):
                    if count == 0:
                        count += 1
                        continue
                    (results, label) = i
                    tuples_labels += [label]


                    # iterate on tuples
                    for t in tuples:
                        # iterate on existing elements of the current tuple
                        keys = t.keys()
                        for e in keys:
                            relationships = t[e].get_relationships()
                            for r in relationships:
                                if r.local_fk_field in ["id", "uuid"]:
                                    continue
                                remote_label_name = r.remote_object_tablename.capitalize()
                                if remote_label_name in indexed_results:
                                    local_value = getattr(t[e], r.local_fk_field)
                                    if local_value is not None:
                                        try:
                                            remote_candidate = indexed_results[remote_label_name][r.remote_object_field][local_value]
                                            t[remote_label_name] = remote_candidate
                                        except Exception as e:
                                            logging.error(e)
                                            traceback.print_exc()
                                            pass
                    tuple_groupby_size = {}
                    for t in tuples:
                        tuple_size = len(t)
                        if not tuple_size in tuple_groupby_size:
                            tuple_groupby_size[tuple_size] = []
                        tuple_groupby_size[tuple_size] += [t]
                    if len(tuple_groupby_size.keys()) > 0:
                        max_size = max(tuple_groupby_size.keys())
                        tuples = tuple_groupby_size[max_size]
                    else:
                        tuples = []

                # reordering tuples
                results = []
                for t in tuples:
                    if len(t) == len(labels):
                        ordered_t = [t[i] for i in labels]
                        results += [tuple(ordered_t)]

                return results


        def extract_sub_row(row, selectables):

            """Adapt a row result to the expectation of sqlalchemy.
            :param row: a list of python objects
            :param selectables: a list entity class
            :return: the response follows what is required by sqlalchemy (if len(model)==1, a single object is fine, in
            the other case, a KeyTuple where each sub object is associated with it's entity name
            """

            if len(selectables) > 1:

                labels = []

                for selectable in selectables:
                    labels += [self.find_table_name(selectable._model).capitalize()]

                product = []
                for label in labels:
                    product = product + [getattr(row, label)]

                # Updating Foreign Keys of objects that are in the row
                for label in labels:
                    current_object = getattr(row, label)
                    metadata = current_object.metadata
                    if metadata and hasattr(metadata, "_fk_memos"):
                        for fk_name in metadata._fk_memos:
                            fks = metadata._fk_memos[fk_name]
                            for fk in fks:
                                local_field_name = fk.column._label
                                remote_table_name = fk._colspec.split(".")[-2].capitalize()
                                remote_field_name = fk._colspec.split(".")[-1]

                                try:
                                    remote_object = getattr(row, remote_table_name)
                                    remote_field_value = getattr(remote_object, remote_field_name)
                                    setattr(current_object, local_field_name, remote_field_value)
                                except:
                                    pass

                # Updating fields that are setted to None and that have default values
                for label in labels:
                    current_object = getattr(row, label)
                    for field in current_object._sa_class_manager:
                        instance_state = current_object._sa_instance_state
                        field_value = getattr(current_object, field)
                        if field_value is None:
                            try:
                                field_column = instance_state.mapper._props[field].columns[0]
                                field_default_value = field_column.default.arg
                                setattr(current_object, field, field_default_value)
                            except:
                                pass

                return KeyedTuple(product, labels=labels)
            else:
                model_name = self.find_table_name(selectables[0]._model).capitalize()
                return getattr(row, model_name)

        import time

        current_milli_time = lambda: int(round(time.time() * 1000))

        part1_starttime = current_milli_time()

        request_uuid = uuid.uuid1()

        labels = []
        columns = set([])
        rows = []

        model_set = extract_models(self._models)

        # get the fields of the join result
        for selectable in model_set:
            labels += [self.find_table_name(selectable._model).capitalize()]

            if selectable._attributes == "*":
                try:
                    selected_attributes = selectable._model._sa_class_manager
                except:
                    selected_attributes = selectable._model.class_._sa_class_manager
                    pass
            else:
                selected_attributes = [selectable._attributes]

            for field in selected_attributes:

                attribute = None
                if hasattr(self._models, "class_"):
                    attribute = selectable._model.class_._sa_class_manager[field].__str__()
                elif hasattr(self._models, "_sa_class_manager"):
                    attribute = selectable._model._sa_class_manager[field].__str__()

                if attribute is not None:
                    columns.add(attribute)
        part2_starttime = current_milli_time()

        # loading objects (from database)
        list_results = []
        for selectable in model_set:
            tablename = self.find_table_name(selectable._model)
            # def filtering_function(n):
            #     print(n.table_name == tablename)
            #     return True
            authorized_secondary_indexes = getattr(selectable._model, "_secondary_indexes", [])
            selected_hints = filter(lambda x: x.table_name == tablename and (x.attribute == "id" or x.attribute in authorized_secondary_indexes), self._hints)
            reduced_hints = map(lambda x:(x.attribute, x.value), selected_hints)
            objects = utils.get_objects(tablename, request_uuid=request_uuid, hints=reduced_hints)
            list_results += [objects]
        part3_starttime = current_milli_time()

        # construct the cartesian product
        tuples = building_tuples(list_results, labels)
        part4_starttime = current_milli_time()

        # filtering tuples (cartesian product)
        indexed_rows = {}
        for product in tuples:
            if len(product) > 0:
                row = KeyedTuple(product, labels=labels)
                row_index_key = "%s" % (str(row))

                if row_index_key in indexed_rows:
                    continue

                all_criterions_satisfied = True

                for criterion in self._criterions:
                    if not criterion.evaluate(row):
                        all_criterions_satisfied = False
                if all_criterions_satisfied:
                    indexed_rows[row_index_key] = True
                    rows += [extract_sub_row(row, model_set)]
        part5_starttime = current_milli_time()

        # reordering tuples (+ selecting attributes)
        final_rows = []
        showable_selection = [x for x in self._models if (not x.is_hidden) or x._is_function]
        part6_starttime = current_milli_time()
        deconverter = JsonDeconverter()
        if self.all_selectable_are_functions():
            final_row = []
            for selection in showable_selection:
                value = selection._function._function(rows)
                final_row += [value]
            return [final_row]
        else:
            for row in rows:
                final_row = []
                for selection in showable_selection:
                    if selection._is_function:
                        value = selection._function._function(rows)
                        final_value = value
                        # final_row += [final_value]
                    else:
                        current_table_name = self.find_table_name(selection._model)
                        key = current_table_name.capitalize()
                        value = None
                        if not utils.is_novabase(row) and has_attribute(row, key):
                            value = get_attribute(row, key)
                        else:
                            value = row
                        if value is not None:
                            if selection._attributes != "*":
                                final_value = get_attribute(value, selection._attributes)
                            else:
                                final_value = value

                    final_row += [final_value]
                if len(showable_selection) == 1:
                    final_rows += final_row
                else:
                    final_rows += [final_row]
        part7_starttime = current_milli_time()

        query_information = """{"building_query": %s, "loading_objects": %s, "building_tuples": %s, "filtering_tuples": %s, "reordering_columns": %s, "selecting_attributes": %s, "description": "%s", "timestamp": %i}""" % (
            part2_starttime - part1_starttime,
            part3_starttime - part2_starttime,
            part4_starttime - part3_starttime,
            part5_starttime - part4_starttime,
            part6_starttime - part5_starttime,
            part7_starttime - part6_starttime,
            str(self),
            current_milli_time()
        )

        logging.info(query_information)
        if file_logger_enabled:
            file_logger.info(query_information)

        from lib.rome.core.lazy_reference import LazyRows
        return LazyRows(final_rows, request_uuid, deconverter)
        # return  final_rows

    def all(self):
        return self.construct_rows()
        # result_list = self.construct_rows()
        #
        # result = []
        # for r in result_list:
        #     ok = True
        #
        #     if ok:
        #         result += [r]
        # return result

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
            tablename = self.find_table_name(row)
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
                    traceback.print_exc()
        _hints = self._hints[:]
        args = self._models + _func + _criterions + _hints + self._initial_models
        return Query(*args)

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
        return Query(*args)

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
        return Query(*args)

    def outerjoin(self, *args, **kwargs):
        return self.join(*args, **kwargs)

    def options(self, *args):
        _func = self._funcs[:]
        _models = self._models[:]
        _criterions = self._criterions[:]
        _initial_models = self._initial_models[:]
        _hints = self._hints[:]
        args = _models + _func + _criterions + _hints + _initial_models
        return Query(*args)

    def order_by(self, *criterion):
        _func = self._funcs[:]
        _models = self._models[:]
        _criterions = self._criterions[:]
        _initial_models = self._initial_models[:]
        _hints = self._hints[:]
        args = _models + _func + _criterions + _hints + _initial_models
        return Query(*args)

    def with_lockmode(self, mode):
        return self


    def subquery(self):
        _func = self._funcs[:]
        _models = self._models[:]
        _criterions = self._criterions[:]
        _initial_models = self._initial_models[:]
        _hints = self._hints[:]
        args = _models + _func + _criterions + _hints + _initial_models
        return Query(*args).all()

    def __iter__(self):
        return iter(self.all())

    def __repr__(self):
        return """{\\"models\\": \\"%s\\", \\"criterions\\": \\"%s\\", \\"hints\\": \\"%s\\"}""" % (self._models, self._criterions, self._hints)
