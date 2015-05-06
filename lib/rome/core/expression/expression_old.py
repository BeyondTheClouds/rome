__author__ = 'jonathan'

import datetime
import pytz

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
                    return getattr(obj, attr.replace("\"", ""))
                else:
                    current_key = attr[:attr.index(".")]
                    next_key = attr[attr.index(".") + 1:]
                    if hasattr(obj, current_key):
                        current_object = getattr(obj, current_key)
                    elif hasattr(obj, current_key.capitalize()):
                        current_object = getattr(obj, current_key.capitalize())
                    elif hasattr(obj, uncapitalize(current_key)):
                        current_object = getattr(obj, uncapitalize(current_key))
                    else:
                        current_object = getattr(obj, current_key)

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