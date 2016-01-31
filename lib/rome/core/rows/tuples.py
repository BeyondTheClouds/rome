import re
from lib.rome.core.models import get_model_classname_from_tablename
import pandas as pd
import math
import traceback
import datetime

from lib.rome.core.utils import DATE_FORMAT

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

def default_panda_building_tuples(lists_results, labels, criterions, hints=[]):

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


def sql_panda_building_tuples(lists_results, labels, criterions, hints=[], metadata={}):

    """ Build tuples (join operator in relational algebra). """

    """ Initializing data indexes. """
    table_id_index = {}
    table_index = {}
    i = 0
    for label in labels:
        table_id_index[label] = {}
        table_index[label] = 1
        i += 1

    i = 0
    for (label, list_results) in zip(labels, lists_results):
        for result in list_results:
            id = result["id"]
            table_id_index[label][id] = result
            pass
        i += 1

    """ Collecting dependencies. """
    joining_pairs = []
    non_joining_criterions = []
    _joining_pairs_str_index = {}
    needed_columns = {}
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

    """ Cloning lists_results. """
    for label in labels:
        needed_columns[label] = ["id"]
    for criterion in non_joining_criterions:
        word_pattern = "[_a-z_A-Z][_a-z_A-Z0-9]*"
        property_pattern = "%s\.%s" % (word_pattern, word_pattern)
        for match in re.findall(property_pattern, str(criterion)):
            table = match.split(".")[0]
            attribute = match.split(".")[1]
            if attribute not in needed_columns[table]:
                needed_columns[table] += [attribute]
    for pair in joining_pairs:
        for each in pair:
            table = each.split(".")[0]
            attribute = each.split(".")[1]
            if attribute not in needed_columns[table]:
                needed_columns[table] += [attribute]

    """ Preparing the query for pandasql. """
    attribute_clause = ",".join(map(lambda x: "%s.id" % (x), labels))
    from_clause = " join ".join(labels)
    where_join_clause = " and ".join(map(lambda x: "%s == %s" % (x[0], x[1]), joining_pairs))
    where_criterions_clause = " and ".join(map(lambda x: str(x), non_joining_criterions))

    where_clause = "1==1 "
    if where_join_clause != "":
        where_clause += " and %s" % (where_join_clause)
    if where_criterions_clause != "":
        where_clause += " and %s" % (where_criterions_clause)
    sql_query = "SELECT %s FROM %s WHERE %s" % (attribute_clause, from_clause, where_clause)
    # print(sql_query)
    metadata["sql"] = sql_query

    """ Preparing Dataframes. """
    env = {}
    for (label, list_results) in zip(labels, lists_results):
        dataframe = pd.DataFrame(data=list_results)
        try:
            dataframe = dataframe[needed_columns[label]]
        except Exception as e:
            # traceback.print_exc(e)
            return []
        dataframe.columns = map(lambda c: "%s__%s" % (label, c), needed_columns[label])
        env[label] = dataframe

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

            if tablename_1 not in env or tablename_2 not in env:
                return []
            dataframe_1 = env[tablename_1] if not tablename_1 in processed_tables else result
            dataframe_2 = env[tablename_2] if not tablename_2 in processed_tables else result

            refactored_attribute_1 = attribute_1.split(".")[0]+"__"+attribute_1.split(".")[1]
            refactored_attribute_2 = attribute_2.split(".")[0]+"__"+attribute_2.split(".")[1]

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

    """ Fixing none result. """
    if result is None:
        if len(labels) == 0:
            return []
        result = env[labels[0]]

    """ Update where clause. """
    new_where_clause = where_clause
    new_where_clause = " ".join(new_where_clause.split())
    new_where_clause = new_where_clause.replace("1==1 and", "")
    new_where_clause = new_where_clause.replace("is None", "== 0")
    new_where_clause = new_where_clause.replace("is not None", "!= 0")

    # <Quick fix for dates>
    fix_date = False
    for non_joining_criterion in non_joining_criterions:
        if "_at " in str(non_joining_criterion):
            fix_date = True
    if fix_date:
        for col in result:
            if col.endswith("_at"):
                def extract_value(x):
                    date_value = None
                    if isinstance(x, dict) and u"value" in x:
                        date_value = str(x[u"value"])
                    if isinstance(x, dict) and "value" in x:
                        date_value = x["value"]
                    if date_value is not None:
                        date_object = datetime.datetime.strptime(date_value, DATE_FORMAT)
                        return (date_object-datetime.datetime(1970, 1, 1)).total_seconds()
                    return x
                result[col] = result[col].apply(lambda x: extract_value(x))
    # </Quick fix for dates>

    for table in needed_columns:
        for attribute in needed_columns[table]:
            old_pattern = "%s.%s" % (table, attribute)
            new_pattern = "%s__%s" % (table, attribute)
            new_where_clause = new_where_clause.replace(old_pattern, new_pattern)

    """ Filter data according to where clause. """
    result = result.fillna(value=0)
    filtered_result = result.query(new_where_clause) if new_where_clause != "1==1" else result

    """ Transform pandas data into dict. """
    final_columns = list(set(map(lambda l: "%s__id" % (l), labels)).intersection(filtered_result))
    final_tables = map(lambda x: x.split("__")[0], final_columns)
    filtered_result = filtered_result[final_columns]
    rows = []
    for each in filtered_result.itertuples():
        try:
            row = []
            for (x, y) in zip(reversed(final_tables), reversed(each)):
                    # row += [table_id_index[x][int(y)]]
                    row += [table_id_index[x][y]]
        except Exception as e:
            traceback.print_exc()
            pass
        rows += [row]
    return rows