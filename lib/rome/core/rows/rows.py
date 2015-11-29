__author__ = 'jonathan'

import itertools
import logging
import traceback
import time

from sqlalchemy.util._collections import KeyedTuple
from sqlalchemy.sql.expression import BinaryExpression

import uuid
from lib.rome.core.utils import get_objects, is_novabase

from lib.rome.core.models import get_model_classname_from_tablename, get_model_class_from_name

from lib.rome.core.lazy import LazyValue

from lib.rome.core.dataformat import get_decoder
from lib.rome.utils.SecondaryIndexDecorator import SECONDARY_INDEXES

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


def all_selectable_are_functions(models):
    return all(x._is_function for x in [y for y in models if not y.is_hidden])

def has_attribute(obj, key):
    if type(obj) is dict:
        return key in obj
    else:
        return hasattr(obj, key)

def set_attribute(obj, key, value):
    if type(obj) is dict:
        obj[key] = value
    else:
        return setattr(obj, key, value)

def get_attribute(obj, key, default=None):
    if type(obj) is dict:
        return obj[key] if key in obj else default
    else:
        return getattr(obj, key, default)

def find_table_name(model):
    """This function return the name of the given model as a String. If the
    model cannot be identified, it returns "none".
    :param model: a model object candidate
    :return: the table name or "none" if the object cannot be identified
    """
    if has_attribute(model, "__tablename__"):
        return model.__tablename__
    if has_attribute(model, "table"):
        return model.table.name
    if has_attribute(model, "class_"):
        return model.class_.__tablename__
    if has_attribute(model, "clauses"):
        for clause in model.clauses:
            return find_table_name(clause)

    return "none"




def extract_models(l):
    already_processed = set()
    result = []
    for selectable in [x for x in l if not x._is_function]:
        if not selectable._model in already_processed:
            already_processed.add(selectable._model)
            result += [selectable]
    return result

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
            labels += [find_table_name(selectable._model)]
        product = []
        for label in labels:
            product = product + [get_attribute(row, label)]
        return KeyedTuple(product, labels=labels)
    else:
        model_name = find_table_name(selectables[0]._model)
        return get_attribute(row, model_name)


def intersect(b1, b2):
    return [val for val in b1 if val in b2]

def flatten(lis):
    """Given a list, possibly nested to any level, return it flattened."""
    new_lis = []
    for item in lis:
        if type(item) == type([]):
            new_lis.extend(flatten(item))
        else:
            new_lis.append(item)
    return new_lis

# def flatten(l):
#     return [item for sublist in l for item in sublist]

def extract_table_data(term):
    term_value = str(term)
    if "." in term_value:
        return {"table": term_value.split(".")[0], "column": term_value.split(".")[1]}
    else:
        return None

def extract_joining_criterion(exp):
    from lib.rome.core.expression.expression import BooleanExpression
    if type(exp) is BooleanExpression:
        return map(lambda x:extract_joining_criterion(x), exp.exps)
    elif type(exp) is BinaryExpression:
        return [[extract_table_data(exp.left)] + [extract_table_data(exp.right)]]
    else:
        return []

def extract_joining_criterion_from_relationship(rel, local_table):
    local_tabledata = {"table": local_table, "column": rel.local_fk_field}
    remote_tabledata = {"table": rel.remote_object_tablename, "column": rel.remote_object_field}
    return [local_tabledata, remote_tabledata]

def building_tuples(list_results, labels, criterions, hints=[]):
    from lib.rome.core.rows.rows import get_attribute, set_attribute, has_attribute
    mode = "experimental"
    if mode is "cartesian_product":
        cartesian_product = []
        for element in itertools.product(*list_results):
            cartesian_product += [element]
        return cartesian_product
    elif mode is "experimental":
        steps = zip(list_results, labels)
        candidates_values = {}
        candidates_per_table = {}
        joining_criterions = []
        non_joining_criterions = {}
        """ Initialising candidates per table """
        for each in labels:
            candidates_per_table[each] = {}
        """ Collecting non-joining expressions """
        for criterion in criterions:
            for exp in criterion.exps:
                for joining_criterion in extract_joining_criterion(exp):
                    foo = [x for x in joining_criterion if x is not None]
                    if len(foo) > 1:
                        joining_criterions += [foo]
                    else:
                        """ Extract here non joining criterions, and use it to filter objects
                            that are located in list_results """
                        exp_criterions = ([x for x in flatten(joining_criterion) if x is not None])
                        for non_joining_criterion in exp_criterions:
                            tablename = non_joining_criterion["table"]
                            column = non_joining_criterion["column"]
                            if not tablename in non_joining_criterions:
                                non_joining_criterions[tablename] = []
                            non_joining_criterions[tablename] += [{
                                "tablename": tablename,
                                "column": column,
                                "exp": exp,
                                "criterion": criterion
                            }]
        """ Collecting joining expressions """
        done_index = {}
        for step in steps:
            tablename = step[1]
            model_classname = get_model_classname_from_tablename(tablename)
            fake_instance = get_model_class_from_name(model_classname)()
            relationships = fake_instance.get_relationships()
            for r in relationships:
                criterion = extract_joining_criterion_from_relationship(r, tablename)
                key1 = criterion[0]["table"]+"__"+criterion[1]["table"]
                key2 = criterion[1]["table"]+"__"+criterion[0]["table"]
                if key1 not in done_index and key2 not in criterion[0]["table"] in labels and criterion[1]["table"] in labels:
                    joining_criterions += [criterion]
                    done_index[key1] = True
                    done_index[key2] = True
                pass
        """ Collecting for each of the aforementioned expressions, its values <-> objects """
        if len(joining_criterions) > 0:
            for criterion in joining_criterions:
                for each in criterion:
                    key = "%s.%s" % (each["table"], each["column"])
                    index_list_results = labels.index(each["table"])
                    objects = list_results[index_list_results]
                    if not candidates_values.has_key(key):
                        candidates_values[key] = {}
                    for object in objects:
                        value_key = get_attribute(object, each["column"])
                        skip = False
                        for hint in hints:
                            if each["table"] == hint.table_name and hint.attribute in object and object[hint.attribute] != hint.value:
                                skip = True
                                break
                        if not skip:
                            if not candidates_values[key].has_key(value_key):
                                candidates_values[key][value_key] = {}
                            object_hash = str(object).__hash__()
                            object_table = object["_nova_classname"] if "_nova_classname" in object else object["nova_classname"]
                            candidates_values[key][value_key][object_hash] = {"value": value_key, "object": object}
                            candidates_per_table[object_table][object_hash] = object
        else:
            for each in steps:
                for each_object in each[0]:
                    object_hash = str(each_object).__hash__()
                    object_table = each_object["_nova_classname"] if "_nova_classname" in each_object else each_object["nova_classname"]
                    candidates_per_table[object_table][object_hash] = each_object
        """ Progressively reduce the list of results """
        results = []
        processed_models = []
        if len(steps) > 0:
            step = steps[0]
            results = map(lambda  x: [candidates_per_table[step[1]][x]], candidates_per_table[step[1]])
            # Apply a filter on the current results
            # validated_results = results
            # for each in non_joining_criterions[step[1]]:
            #     validated_results = filter(lambda r: each["criterion"].evaluate(r[0], additional_parameters={"fixed_ips": r[0]}), validated_results)
            # results = validated_results
            processed_models += [step[1]]
        remaining_models = map(lambda x:x[1], steps[1:])
        for step in steps[1:]:
            for criterion in joining_criterions:
                criterion_models = map(lambda x: x["table"], criterion)
                candidate_models = [step[1]] + processed_models
                if len(intersect(candidate_models, criterion_models)) > 1:
                    processed_models += [step[1]]
                    remaining_models = filter(lambda x: x ==step[1], remaining_models)
                    current_criterion_option = filter(lambda x:x["table"]==step[1], criterion)
                    remote_criterion_option = filter(lambda x:x["table"]!=step[1], criterion)
                    if not (len(current_criterion_option) > 0 and len(remote_criterion_option) > 0):
                        continue
                    current_criterion_part = current_criterion_option[0]
                    remote_criterion_part = remote_criterion_option[0]
                    new_results = []
                    for each in results:
                        existing_tuple_index = processed_models.index(remote_criterion_part["table"])
                        existing_value = get_attribute(each[existing_tuple_index], remote_criterion_part["column"])
                        if existing_value is not None:
                            key = "%s.%s" % (current_criterion_part["table"], current_criterion_part["column"])
                            candidates_value_index = candidates_values[key]
                            candidates = candidates_value_index[existing_value] if existing_value in candidates_value_index else {}
                            for candidate_key in candidates:
                                new_results += [each + [candidates[candidate_key]["object"]]]
                    results = new_results
                    break
                continue
        return results

def wrap_with_lazy_value(value, only_if_necessary=True, request_uuid=None):
    if only_if_necessary and type(value).__name__ in ["int", "str", "float", "unicode"]:
        return value
    elif type(value) is dict and "timezone" in value:
        decoder = get_decoder()
        return decoder.desimplify(value)
    else:
        return LazyValue(value, request_uuid)


def construct_rows(models, criterions, hints, session=None, request_uuid=None):

    """This function constructs the rows that corresponds to the current orm.
    :return: a list of row, according to sqlalchemy expectation
    """

    current_milli_time = lambda: int(round(time.time() * 1000))

    part1_starttime = current_milli_time()

    if request_uuid is None:
        request_uuid = uuid.uuid1()
    else:
        request_uuid = request_uuid

    labels = []
    columns = set([])
    rows = []

    model_set = extract_models(models)

    """ Get the fields of the join result """
    for selectable in model_set:
        labels += [find_table_name(selectable._model)]
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
            if has_attribute(models, "class_"):
                attribute = selectable._model.class_._sa_class_manager[field].__str__()
            elif has_attribute(models, "_sa_class_manager"):
                attribute = selectable._model._sa_class_manager[field].__str__()
            if attribute is not None:
                columns.add(attribute)
    part2_starttime = current_milli_time()

    """ Loading objects (from database) """
    list_results = []
    for selectable in model_set:
        tablename = find_table_name(selectable._model)
        authorized_secondary_indexes = get_attribute(selectable._model, "_secondary_indexes", [])
        # tablename = selectable._model.__tablename__
        # authorized_secondary_indexes = SECONDARY_INDEXES[tablename] if tablename in SECONDARY_INDEXES else []
        selected_hints = filter(lambda x: x.table_name == tablename and (x.attribute == "id" or x.attribute in authorized_secondary_indexes), hints)
        reduced_hints = map(lambda x:(x.attribute, x.value), selected_hints)
        objects = get_objects(tablename, request_uuid=request_uuid, skip_loading=False, hints=reduced_hints)
        list_results += [objects]
    part3_starttime = current_milli_time()

    """ Building tuples """
    tuples = building_tuples(list_results, labels, criterions, hints)
    part4_starttime = current_milli_time()

    """ Filtering tuples (cartesian product) """
    indexed_rows = {}
    for product in tuples:
        if len(product) > 0:
            row = KeyedTuple(product, labels=labels)
            row_index_key = "%s" % (str(row))

            if row_index_key in indexed_rows:
                continue

            all_criterions_satisfied = True

            for criterion in criterions:
                if not criterion.evaluate(row):
                    all_criterions_satisfied = False
                    break
            if all_criterions_satisfied:
                indexed_rows[row_index_key] = True
                rows += [extract_sub_row(row, model_set)]
    part5_starttime = current_milli_time()
    deconverter = get_decoder(request_uuid=request_uuid)

    """ Reordering tuples (+ selecting attributes) """
    final_rows = []
    showable_selection = [x for x in models if (not x.is_hidden) or x._is_function]
    part6_starttime = current_milli_time()

    """ Selecting attributes """
    if all_selectable_are_functions(models):
        final_row = []
        for selection in showable_selection:
            value = selection._function._function(rows)
            final_row += [value]
        final_row = map(lambda x: deconverter.desimplify(x), final_row)
        return [final_row]
    else:
        for row in rows:
            final_row = []
            for selection in showable_selection:
                if selection._is_function:
                    value = selection._function._function(rows)
                    final_row += [value]
                else:
                    current_table_name = find_table_name(selection._model)
                    key = current_table_name
                    if not is_novabase(row) and has_attribute(row, key):
                        value = get_attribute(row, key)
                    else:
                        value = row
                    if value is not None:
                        if selection._attributes != "*":
                            final_row += [get_attribute(value, selection._attributes)]
                        else:
                            final_row += [value]
            final_row = map(lambda x: wrap_with_lazy_value(x, request_uuid=request_uuid), final_row)

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
        """{\\"models\\": \\"%s\\", \\"criterions\\": \\"%s\\"}""" % (models, criterions),
        current_milli_time()
    )

    logging.info(query_information)
    if file_logger_enabled:
        file_logger.info(query_information)

    return final_rows