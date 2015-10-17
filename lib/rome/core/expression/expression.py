__author__ = 'jonathan'

import datetime
import pytz
from lib.rome.core.dataformat import get_decoder
import re
import uuid
from sqlalchemy.sql.expression import BinaryExpression

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
        self.deconverter = get_decoder()

    def __getattr__(self, item):
        deconverted_value = self.deconverter.desimplify(self.entries[item])
        return deconverted_value

boolean_expression_str_memory = {}

class BooleanExpression(object):
    def __init__(self, operator, *exps):
        def transform_exp(exp):
            if type(exp) is not BooleanExpression and self.operator != "NORMAL":
                return BooleanExpression("NORMAL", exp)
            else:
                return exp
        self.operator = operator
        self.exps = map(lambda x: transform_exp(x), exps)
        self.deconverter = get_decoder()
        self.compiled_expression = ""
        self.uuid = str(uuid.uuid1()).replace("-", "")
        # prepare the expression
        self.variable_substitution_dict = {}
        self.default_value_dict = {}
        self.prepare_expression()

    def is_boolean_expression(self):
        return True

    def prepare_expression(self):

        def collect_expressions(exp):
            if type(exp) is BooleanExpression:
                return exp.compiled_expression
            if type(exp) is BinaryExpression:
                return self.prepare_criterion(exp)
            else:
                return exp

        compiled_expressions = map(lambda x: "(%s)" % (collect_expressions(x)), self.exps)

        joined_compiled_expressions = []
        if self.operator == "AND":
            joined_compiled_expressions = " and ".join(compiled_expressions)
        elif self.operator == "OR":
            joined_compiled_expressions = " or ".join(compiled_expressions)
        elif self.operator == "NORMAL":
            joined_compiled_expressions = " or ".join(compiled_expressions)

        self.compiled_expression = joined_compiled_expressions

        for criterion_str in compiled_expressions:
            for expression in self.exps:
                if type(expression) is BinaryExpression:
                    expression_parts = [expression.right, expression.left]
                    for expression_part in expression_parts:
                        if hasattr(expression_part, "default") and expression_part.bind is None and expression_part.default is not None:
                            expression_part.bind = expression_part.default.arg
                        if ":" in str(expression_part):
                            # handle right part of the expression
                            if " in " in criterion_str:
                                count = 1
                                for i in expression_part.element:
                                    corrected_label = ("%s_%s_%i" % (i._orig_key, self.uuid, count))
                                    key = ":%s_%i" % (i._orig_key, count)
                                    self.variable_substitution_dict[key] = corrected_label
                                    self.default_value_dict[corrected_label] = i.value
                                    count += 1
                            elif not "." in str(expression_part):
                                original_label = str(expression_part)
                                corrected_label = ("%s_%s" % (original_label, self.uuid)).replace(":", "")
                                self.variable_substitution_dict[original_label] = corrected_label
                                self.default_value_dict[corrected_label] = expression_part.value

        for sub in self.variable_substitution_dict:
            joined_compiled_expressions = joined_compiled_expressions.replace(sub, self.variable_substitution_dict[sub])
        joined_compiled_expressions = joined_compiled_expressions.replace(":", "")

        for exp in self.exps:
            if type(exp) is BooleanExpression:
                for default_value_key in exp.default_value_dict:
                    self.default_value_dict[default_value_key] = exp.default_value_dict[default_value_key]

        self.compiled_expression = joined_compiled_expressions
        return self.compiled_expression

    def prepare_criterion(self, criterion):
        criterion_str = criterion.__str__()

        if criterion_str in boolean_expression_str_memory:
            criterion_str = boolean_expression_str_memory[criterion_str]
        else:
            prev_criterion_str = criterion_str

            subs = {
                " = ": " == ",
                # ":": "",
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

            for sub in self.variable_substitution_dict:
                criterion_str = criterion_str.replace(sub, self.variable_substitution_dict[sub])

            # handle regex
            if "REGEXP" in criterion_str:
                tab = criterion_str.split("REGEXP")
                a = tab[0]
                b = tab[1]
                criterion_str = ("""__import__('re').search(%s, %s) is not None\n""" % (b, a))

            boolean_expression_str_memory[prev_criterion_str] = criterion_str

        return criterion_str

    def evaluate(self, value):

        # construct a dict with the values involved in the expression
        values_dict = {}
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

        for key in self.default_value_dict:
            values_dict[key] = self.default_value_dict[key]
        final_values_dict = {}
        for key in values_dict:
            value = values_dict[key]
            if key.startswith("id_"):
                value = int(value)
            final_values_dict[key] = value
        for key in values_dict:
            if key in self.variable_substitution_dict:
                value = values_dict[key]
                if key.startswith("id_"):
                    value = int(value)
                final_values_dict[self.variable_substitution_dict[key]] = value
        for expression in self.exps:
            if type(expression) is BinaryExpression:
                expression_parts = [expression.right, expression.left]
                for expression_part in expression_parts:
                    if hasattr(expression_part, "default") and expression_part.default is not None:
                        key = str(expression_part).split(".")[0]
                        attr = str(expression_part).split(".")[1]
                        if getattr(final_values_dict[key], attr, None) is None:
                            value = expression_part.default.arg
                            setattr(final_values_dict[key], attr, value)
        try:
            result = eval(self.compiled_expression, final_values_dict)
        except:
            import traceback
            traceback.print_exc()
            if self.operator == "NORMAL":
                return False
            for exp in self.exps:
                if exp.evaluate(value):
                    if self.operator in ["OR"]:
                        return True
                else:
                    if self.operator in ["AND"]:
                        return False
            if self.operator in ["NORMAL", "OR"]:
                return False
            else:
                return True
            pass
        if result is False:
            toto = 1
        return result
