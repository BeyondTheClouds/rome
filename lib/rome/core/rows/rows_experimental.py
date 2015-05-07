__author__ = 'jonathan'

import itertools

from lib.rome.core.expression.expression import *
from sqlalchemy.sql.expression import BinaryExpression

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

def building_tuples(list_results, labels, criterions):
    from lib.rome.core.rows.rows import get_attribute, set_attribute, has_attribute
    mode = "experimental"
    if mode is "cartesian_product":
        cartesian_product = []
        for element in itertools.product(*list_results):
            cartesian_product += [element]
        return cartesian_product
    elif mode is "experimental":
        results_per_table = {}
        filtering_values = {}
        joining_criterions = []
        # Initialising results per table
        for each in labels:
            index_list_results = labels.index(each)
            results_per_table[each] = list_results[index_list_results][:]
        # Collecting joining expressions
        for criterion in criterions:
            # if criterion.operator in  "NORMAL":
            for exp in criterion.exps:
                for joining_criterion in extract_joining_criterion(exp):
                    foo = [x for x in joining_criterion if x is not None]
                    if len(foo) > 1:
                        joining_criterions += [foo]
        # Collecting for each of the aforementioned expressions, its values <-> objects
        for criterion in joining_criterions:
            for each in criterion:
                key = "%s.%s" % (each["table"], each["column"])
                index_list_results = labels.index(each["table"])
                objects = list_results[index_list_results]
                if not filtering_values.has_key(key):
                    filtering_values[key] = {}
                for object in objects:
                    value_key = get_attribute(object, each["column"])
                    if not filtering_values[key].has_key(value_key):
                        filtering_values[key][value_key] = []
                    filtering_values[key][value_key] += [{"value": value_key, "object": object}]
        # Progressively reduce the list of results
        for criterion in joining_criterions:
            key_left = "%s.%s" % (criterion[0]["table"], criterion[0]["column"])
            key_right = "%s.%s" % (criterion[1]["table"], criterion[1]["column"])
            common_values = intersect(filtering_values[key_left].keys(), filtering_values[key_right].keys())
            left_objects_ok = flatten(map(lambda x:filtering_values[key_left][x], common_values))
            right_objects_ok = flatten(map(lambda x:filtering_values[key_right][x], common_values))
            results_per_table[criterion[0]["table"]] = intersect(results_per_table[criterion[0]["table"]], map(lambda x:x["object"], left_objects_ok))
            results_per_table[criterion[1]["table"]] = intersect(results_per_table[criterion[1]["table"]], map(lambda x:x["object"], right_objects_ok))
        # Build the cartesian product
        results = []
        steps = zip(list_results, labels)
        processed_models = []
        if len(steps) > 0:
            step = steps[0]
            results = map(lambda x:[x], results_per_table[step[1]])
            processed_models += [step[1]]
        remaining_models = map(lambda x:x[1], steps[1:])
        for step in steps[1:]:
            for criterion in joining_criterions:
                criterion_models = map(lambda x:x["table"], criterion)
                candidate_models = [step[1]] + processed_models
                if len(intersect(candidate_models, criterion_models)) > 1:
                    processed_models += [step[1]]
                    remaining_models = filter(lambda x:x ==step[1], remaining_models)
                    try:
                        current_criterion_part = filter(lambda x:x["table"]==step[1], criterion)[0]
                        remote_criterion_part = filter(lambda x:x["table"]!=step[1], criterion)[0]
                        new_results = []
                        for each in results:
                            existing_tuple_index = processed_models.index(remote_criterion_part["table"])
                            existing_value = each[existing_tuple_index][remote_criterion_part["column"]]
                            key = "%s.%s" % (current_criterion_part["table"], current_criterion_part["column"])
                            candidates = filtering_values[key][existing_value]
                            for candidate in candidates:
                                new_results += [each + [candidate["object"]]]
                        results = new_results
                    except:
                        pass
                continue
        return results