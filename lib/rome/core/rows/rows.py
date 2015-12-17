__author__ = 'jonathan'

import logging
import time
import uuid

import pandas as pd
from sqlalchemy.sql.expression import BinaryExpression
from sqlalchemy.util._collections import KeyedTuple

from lib.rome.core.dataformat import get_decoder
from lib.rome.core.lazy import LazyValue
from lib.rome.core.utils import get_objects, is_novabase

from lib.rome.core.models import get_model_classname_from_tablename

import math
import re

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

def correct_boolean_int(expression_str):
    expression_str = expression_str.replace("___deleted == 0", "___deleted != 1")
    return expression_str

def correct_expression_containing_none(expression_str):
    # Use trick found here: http://stackoverflow.com/questions/26535563/querying-for-nan-and-other-names-in-pandas
    terms = expression_str.split()
    clean_expression = " ".join(terms)
    if len(terms) > 0:
        variable_name = terms[0]
        variable_name = variable_name.replace("(", "")
        if "is not None" in clean_expression:
            clean_expression = "(%s == %s)" % (variable_name, variable_name, variable_name)
        if "is None" in clean_expression:
            clean_expression = "(%s != %s)" % (variable_name, variable_name, variable_name)
    return clean_expression

def drop_y(df):
    # list comprehension of the cols that end with '_y'
    to_drop = [x for x in df if x.endswith('_y')]
    df.drop(to_drop, axis=1, inplace=True)

def rename_x(df):
    cols = list(df.columns)
    fixed_columns = map(lambda x: re.sub("_x$", "", x), cols)
    df.columns = fixed_columns

def building_tuples(lists_results, labels, criterions, hints=[]):

    """ Build tuples (join operator in relational algebra). """

    """ Create the Dataframe indexes. """
    dataframes = []
    dataindex = {}
    substitution_index = {}
    normal_keys_index = {}
    refactored_keys_index = {}
    normal_keys_to_key_index = {}
    refactored_keys_to_key_index = {}
    refactored_keys_to_table_index = {}
    index = 0


    classname_index = {}
    for each in labels:
        classname_index[each] = get_model_classname_from_tablename(each)
    # if len(lists_results) == 1:
    #     return map(lambda x: [x], lists_results[0])


    for list_results in lists_results:
        label = labels[index]
        if len(list_results) == 0:
            continue
        keys = map(lambda x: x, list_results[0]) + ["created_at", "updated_at"]

        dataframe = pd.DataFrame(data=list_results, columns=keys)

        for value in keys:
            normal_key = "%s.%s" % (label, value)
            refactored_keys = "%s___%s" % (label, value)
            refactored_keys_to_table_index[refactored_keys] = label
            normal_keys_to_key_index[normal_key] = value
            refactored_keys_to_key_index[refactored_keys] = value
        normal_keys = map(lambda x: "%s.%s" % (label, x), keys)
        normal_keys_index[label] = normal_keys
        refactored_keys = map(lambda x: "%s___%s" % (label, x), keys)
        refactored_keys_index[label] = refactored_keys
        for (a, b) in zip(normal_keys, refactored_keys):
            substitution_index[a] = b
        dataframe.columns = refactored_keys
        dataframes += [dataframe]

        """ Index the dataframe and create a reverse index. """
        dataindex[label] = index
        index += 1

    """ Collecting joining expressions. """
    joining_pairs = []
    non_joining_criterions = []
    _joining_pairs_str_index = {}
    _nonjoining_criterions_str_index = {}
    for criterion in criterions:
        _joining_pairs = criterion.extract_joining_pairs()
        _nonjoining_criterions = criterion.extract_nonjoining_criterions()

        _nonjoining_criterions_str = str(_nonjoining_criterions)

        if len(_joining_pairs) > 0:
            _joining_pairs_str = str(sorted(_joining_pairs[0]))
            if not _joining_pairs_str in _joining_pairs_str_index:
                _joining_pairs_str_index[_joining_pairs_str] = 1
                joining_pairs += _joining_pairs
        if not _nonjoining_criterions_str in _nonjoining_criterions_str_index:
            _nonjoining_criterions_str_index[_nonjoining_criterions_str] = 1
            non_joining_criterions += _nonjoining_criterions



    """ Construct the resulting rows. """
    if len(labels) > 1 and len(filter(lambda x: len(x) == 0, lists_results)) > 0:
        return []

    result = None

    if len(lists_results) > 1:
        processed_tables = []
        for joining_pair in joining_pairs:
            """ Preparing the tables that will be joined. """


            attribute_1 = joining_pair[0].strip()
            attribute_2 = joining_pair[1].strip()
            tablename_1 = attribute_1.split(".")[0]
            tablename_2 = attribute_2.split(".")[0]

            if tablename_1 not in dataindex or tablename_2 not in dataindex:
                return []
            index_1 = dataindex[tablename_1]
            index_2 = dataindex[tablename_2]
            dataframe_1 = dataframes[index_1] if not tablename_1 in processed_tables else result
            dataframe_2 = dataframes[index_2] if not tablename_2 in processed_tables else result

            refactored_attribute_1 = attribute_1.split(".")[0]+"___"+attribute_1.split(".")[1]
            refactored_attribute_2 = attribute_2.split(".")[0]+"___"+attribute_2.split(".")[1]

            """ Join the tables. """
            try:
                result = pd.merge(dataframe_1, dataframe_2, left_on=refactored_attribute_1, right_on=refactored_attribute_2, how="outer")
                drop_y(result)
                rename_x(result)
            except KeyError:
                return []
            """ Update the history of processed tables. """
            processed_tables += [tablename_1, tablename_2]
            processed_tables = list(set(processed_tables))

    """ Filtering rows. """
    if result is None:
        if len(dataframes) == 0:
            return []
        result = dataframes[0]

    for non_joining_criterion in non_joining_criterions:
        expression_str = non_joining_criterion.raw_expression
        for value in substitution_index:
            if value in expression_str:
                corresponding_key = substitution_index[value]
                expression_str = expression_str.replace(value, corresponding_key)
        try:
            corrected_expression = correct_boolean_int(expression_str)
            corrected_expression = correct_expression_containing_none(corrected_expression)
            result = result.query(corrected_expression)
        except:
            pass

    """ Building the rows. """
    rows = []
    # for index, row in result.iterrows():
    #     value = row
    #     row = []
    #     for label in labels:
    #         refactored_keys = refactored_keys_index[label] if label in refactored_keys_index else {}
    #         sub_row = {}
    #         for refactored_key in refactored_keys:
    #             raw_key = refactored_keys_to_key_index[refactored_key]
    #             sub_row[raw_key] = value[refactored_key] if refactored_key in value else None
    #             if type(sub_row[raw_key]) is float and math.isnan(sub_row[raw_key]):
    #                 sub_row[raw_key] = None
    #             sub_row[raw_key] = value[refactored_key] if refactored_key in value else 0
    #         row += [sub_row]
    #     rows += [row]
    # return rows
    #
    # rows = []
    # for value in result.values():

    # def row_to_dict()

    # for index, row in result.iterrows():
    # for row in result.itertuples():
    #     value = row
    columns_indexes = {}
    label_indexes = {}
    i = 0
    for refactored_key in result.columns.values:
        columns_indexes[refactored_key] = i
        i += 1
    i = 0
    for label in labels:
        label_indexes[label] = i
        i += 1

    transposed_result = result.transpose()
    dict_values = transposed_result.to_dict()
    for value in dict_values.values():
        row = map(lambda x: {}, labels)
        for ci in value:
            table = refactored_keys_to_table_index[ci]
            table_index = label_indexes[table]
            key = refactored_keys_to_key_index[ci]
            v = value[ci]
            if type(v) is float and math.isnan(v):
                v = 0
            if key == "_metadata_novabase_classname":
                v = classname_index[table]
            row[table_index][key] = v
        rows += [row]
    return rows

def wrap_with_lazy_value(value, only_if_necessary=True, request_uuid=None):
    if value is None:
        return None
    if only_if_necessary and type(value).__name__ in ["int", "str", "float", "unicode"]:
        return value
    elif type(value) is dict and "timezone" in value:
        decoder = get_decoder()
        return decoder.desimplify(value)
    else:
        return LazyValue(value, request_uuid)


# def wrap_with_lazy_value(value, only_if_necessary=True, request_uuid=None):
#     return LazyValue(value, request_uuid)

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

            # for criterion in criterions:
            #     if not criterion.is_joining_expression and not criterion.evaluate(row):
            #         all_criterions_satisfied = False
            #         break
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