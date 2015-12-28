__author__ = 'jonathan'

from lib.rome.core.expression.expression import *

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
    return BooleanExpression("and", *exps)


def or_(*exps):
    return BooleanExpression("or", *exps)


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
            if hasattr(row, fieldtable):
                filtered_rows += [getattr(row, fieldtable)]
            else:
                if not type(row) is list:
                    row = [row]
                for subrow in row:
                    table = get_attribute(subrow, "__tablename__", get_attribute(subrow, "_nova_classname", None))
                    if table == fieldtable:
                        filtered_rows += [subrow]
        result = [get_attribute(row, fieldname) for row in filtered_rows]
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
