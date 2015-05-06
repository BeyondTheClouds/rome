__author__ = 'jonathan'

import itertools
import logging
import traceback

from sqlalchemy.util._collections import KeyedTuple

import uuid
from lib.rome.core.utils import get_objects, is_novabase

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


def find_table_name(model):

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
            labels += [find_table_name(selectable._model).capitalize()]

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
        model_name = find_table_name(selectables[0]._model).capitalize()
        return getattr(row, model_name)

def building_tuples(list_results, labels, criterions):
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
                # iterate on existing elements of the current rows
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

def construct_rows(models, criterions, hints):

    """This function constructs the rows that corresponds to the current orm.
    :return: a list of row, according to sqlalchemy expectation
    """

    import time

    current_milli_time = lambda: int(round(time.time() * 1000))

    part1_starttime = current_milli_time()

    request_uuid = uuid.uuid1()

    labels = []
    columns = set([])
    rows = []

    model_set = extract_models(models)

    # get the fields of the join result
    for selectable in model_set:
        labels += [find_table_name(selectable._model).capitalize()]

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
            if hasattr(models, "class_"):
                attribute = selectable._model.class_._sa_class_manager[field].__str__()
            elif hasattr(models, "_sa_class_manager"):
                attribute = selectable._model._sa_class_manager[field].__str__()

            if attribute is not None:
                columns.add(attribute)
    part2_starttime = current_milli_time()

    # loading objects (from database)
    list_results = []
    for selectable in model_set:
        tablename = find_table_name(selectable._model)
        # def filtering_function(n):
        #     print(n.table_name == tablename)
        #     return True
        authorized_secondary_indexes = getattr(selectable._model, "_secondary_indexes", [])
        selected_hints = filter(lambda x: x.table_name == tablename and (x.attribute == "id" or x.attribute in authorized_secondary_indexes), hints)
        reduced_hints = map(lambda x:(x.attribute, x.value), selected_hints)
        objects = get_objects(tablename, request_uuid=request_uuid, hints=reduced_hints)
        list_results += [objects]
    part3_starttime = current_milli_time()

    # construct the cartesian product
    tuples = building_tuples(list_results, labels, criterions)
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

            for criterion in criterions:
                if not criterion.evaluate(row):
                    all_criterions_satisfied = False
            if all_criterions_satisfied:
                indexed_rows[row_index_key] = True
                rows += [extract_sub_row(row, model_set)]
    part5_starttime = current_milli_time()

    # reordering tuples (+ selecting attributes)
    final_rows = []
    showable_selection = [x for x in models if (not x.is_hidden) or x._is_function]
    part6_starttime = current_milli_time()
    if all_selectable_are_functions(models):
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
                    final_row += [value]
                else:
                    current_table_name = find_table_name(selection._model)
                    key = current_table_name.capitalize()
                    value = None
                    if not is_novabase(row) and hasattr(row, key):
                        value = getattr(row, key)
                    else:
                        value = row
                    if value is not None:
                        if selection._attributes != "*":
                            final_row += [getattr(value, selection._attributes)]
                        else:
                            final_row += [value]
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
        "None",
        current_milli_time()
    )

    logging.info(query_information)
    if file_logger_enabled:
        file_logger.info(query_information)

    return final_rows