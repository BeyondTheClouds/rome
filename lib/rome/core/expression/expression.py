__author__ = 'jonathan'

import datetime
import pytz
from lib.rome.core.dataformat.deconverter import JsonDeconverter
import re

from lib.rome.core.rows.rows import get_attribute, has_attribute

def uncapitalize(s):
    return s[:1].lower() + s[1:] if s else ''

def get_attribute_reccursively(obj, attr, otherwise=None):
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

            if type(obj) is dict and next_key in obj:
                return obj[next_key]

            return get_attribute_reccursively(current_object, next_key, otherwise)
    except AttributeError:
        return otherwise

class LazyDictionnary:
    """This temporary class is used to make a dict acting like an object. This code can be found at:
        http://stackoverflow.com/questions/1305532/convert-python-dict-to-object
    """

    def __init__(self, **entries):
        self.entries = entries
        self.deconverter = JsonDeconverter()

    def __getattr__(self, item):
        deconverted_value = self.deconverter.desimplify(self.entries[item])
        return deconverted_value

boolean_expression_str_memory = {}

class BooleanExpression(object):
    def __init__(self, operator, *exps):
        self.operator = operator
        self.exps = exps
        self.deconverter = JsonDeconverter()

    # def is_boolean_expression(self):
    #     return True

    def is_boolean_expression(self):
        return True

    def evaluate_criterion(self, criterion, value):

        # return True
        criterion_str = criterion.__str__()

        if criterion_str in boolean_expression_str_memory:
            criterion_str = boolean_expression_str_memory[criterion_str]
        else:
            prev_criterion_str = criterion_str
            # # replace equality operator
            # criterion_str = criterion_str.replace(" = ", " == ")
            # # remove prefix of arguments
            # criterion_str = criterion_str.replace(":", "")
            # # remove quotes arround attributes
            # criterion_str = criterion_str.replace("\"", "")
            # # replace "IN" operator by "in" operator
            # criterion_str = criterion_str.replace(" IN ", " in ")
            # # replace "IS" operator by "is" operator
            # criterion_str = criterion_str.replace(" IS ", " is ")
            # # replace "NOT" operator by "not" operator
            # criterion_str = criterion_str.replace(" NOT ", " not ")
            # # replace "NULL" operator by "None" operator
            # criterion_str = criterion_str.replace("NULL", "None")
            # # Format correctly lists
            # criterion_str = criterion_str.replace("(", "[")
            # criterion_str = criterion_str.replace(")", "]")

            subs = {
                " = ": " == ",
                ":": "",
                "\"": "",
                "IN": " in ",
                "IS": " is ",
                "NOT": " not ",
                "NULL": "None",
                "(": "[",
                ")": "]"
            }
            compiled = re.compile('|'.join(map(re.escape, subs)))
            criterion_str = compiled.sub(lambda x: subs[x.group(0)], criterion_str)

            # handle regex
            if "REGEXP" in criterion_str:
                tab = criterion_str.split("REGEXP")
                a = tab[0]
                b = tab[1]
                criterion_str = ("""__import__('re').search(%s, %s) is not None\n""" % (b, a))

            boolean_expression_str_memory[prev_criterion_str] = criterion_str

        # return True
        # construct a dict with the values involved in the expression
        values_dict = {}
        # return True
        if type(value) is not dict:
            for key in value.keys():
                try:
                    s = LazyDictionnary(**value[value.keys().index(key)])
                    values_dict[key] = s
                except:
                    print("[BUG] evaluation failed: %s -> %s" % (key, value))
                    return False
        else:
            values_dict = value
        # check if right value is a named argument
        expressions = []
        # return True
        from sqlalchemy.sql.expression import BinaryExpression
        if type(criterion) is BooleanExpression:
            if criterion.operator in ["AND", "OR"]:
                return criterion.evaluate(value)
            else:
                expressions = criterion.exps
        elif type(criterion) is BinaryExpression:
            expressions = [criterion.expression]
        for expression in expressions:
            if ":" in str(expression.right):
                # fix the prefix of the name argument
                if " in " in criterion_str:
                    count = 1
                    for i in expression.right.element:
                        values_dict["%s_%i" % (i._orig_key, count)] = i.value
                        count += 1
                else:
                    corrected_label = str(expression.right).replace(":", "")
                    values_dict[corrected_label] = expression.right.value
        # evaluate the expression thanks to the 'eval' function
        result = False
        try:
            result = eval(criterion_str, values_dict)
        except:
            pass
        return result

    def evaluate_criterion_(self, criterion, value):

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
                p = re.compile(b)
                return p.search(a)
                # return "%s" % (a) == "%s" % (b) or a == b

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
            left_values += [get_attribute_reccursively(value, left.capitalize())]
        left_values = map(lambda x: self.deconverter.desimplify(x), left_values)

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
                        right_value = get_attribute_reccursively(value, right.capitalize())
            elif hasattr(criterion, "is_boolean_expression") and criterion.is_boolean_expression():
                right_value = criterion.evaluate(value)
        right_value = self.deconverter.desimplify(right_value)
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
            left_value = self.deconverter.desimplify(left_value)

            for right_term in right_terms:
                key = "%s" % (right_term._orig_key)
                if has_attribute(right_term.value, key):
                    right_value = get_attribute(right_term.value, key)
                else:
                    right_value = right_term.value
                right_value = self.deconverter.desimplify(right_value)

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

    def get_string_to_evaluate(self, criterion, value):

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
            left_values += [get_attribute_reccursively(value, left.capitalize())]
        left_values = map(lambda x: self.deconverter.desimplify(x), left_values)

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
                        right_value = get_attribute_reccursively(value, right.capitalize())
            elif hasattr(criterion, "is_boolean_expression") and criterion.is_boolean_expression():
                right_value = criterion.evaluate(value)
        right_value = self.deconverter.desimplify(right_value)
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
            left_value = self.deconverter.desimplify(left_value)

            for right_term in right_terms:
                key = "%s" % (right_term._orig_key)
                if has_attribute(right_term.value, key):
                    right_value = get_attribute(right_term.value, key)
                else:
                    right_value = right_term.value
                right_value = self.deconverter.desimplify(right_value)

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