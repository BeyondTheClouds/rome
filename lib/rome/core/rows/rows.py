__author__ = 'jonathan'

import itertools
import logging
import traceback

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