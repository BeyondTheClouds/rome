__author__ = 'jonathan'

import itertools

from lib.rome.core.expression.expression import *
from sqlalchemy.sql.expression import BinaryExpression
from lib.rome.core.utils import current_milli_time

from lib.rome.core.models import get_model_classname_from_tablename, get_model_class_from_name

def intersect(b1, b2):
    return [val for val in b1 if val in b2]

def flatten(l):
    return [item for sublist in l for item in sublist]

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
        # results_per_table = {}
        candidates_values = {}
        candidates_per_table = {}
        joining_criterions = []
        filtering_criterions = []
        # Initialising candidates per table

        time1 = current_milli_time()
        for each in labels:
            candidates_per_table[each] = []
            # index_list_results = labels.index(each)
            # candidates_per_table[each] = list_results[index_list_results][:]
        # Collecting joining expressions
        for criterion in criterions:
            # if criterion.operator in  "NORMAL":
            for exp in criterion.exps:
                for joining_criterion in extract_joining_criterion(exp):
                    foo = [x for x in joining_criterion if x is not None]
                    if len(foo) > 1:
                        joining_criterions += [foo]
        done_index = {}
        # Consolidating joining criterions with data stored in relationships
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
        # Collecting for each of the aforementioned expressions, its values <-> objects
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
                                candidates_values[key][value_key] = []
                            candidates_values[key][value_key] += [{"value": value_key, "object": object}]
                            candidates_per_table[object.base] += [object]
        else:
            for each in steps:
                candidates_per_table[each[1]] = each[0]
            # print("toto?")
        # Progressively reduce the list of results
        results = []
        processed_models = []
        if len(steps) > 0:
            step = steps[0]
            # results = map(lambda x:[x], list_results[0])
            results = map(lambda  x: [x], candidates_per_table[step[1]])
            processed_models += [step[1]]
        remaining_models = map(lambda x:x[1], steps[1:])

        print("here => %s seconds" % (current_milli_time() - time1))
        print("here4??")
        for step in steps[1:]:
            for criterion in joining_criterions:
                criterion_models = map(lambda x: x["table"], criterion)
                candidate_models = [step[1]] + processed_models
                if len(intersect(candidate_models, criterion_models)) > 1:
                    processed_models += [step[1]]
                    remaining_models = filter(lambda x: x ==step[1], remaining_models)
                    # try:
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
                            candidates = candidates_value_index[existing_value] if existing_value in candidates_value_index else []
                            for candidate in candidates:
                                new_results += [each + [candidate["object"]]]
                    results = new_results
                continue
        return results
