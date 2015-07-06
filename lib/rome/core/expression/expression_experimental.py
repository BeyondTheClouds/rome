__author__ = 'jonathan'


from sqlalchemy.sql.expression import BinaryExpression
from lib.rome.core.dataformat.deconverter import JsonDeconverter

class BooleanExpression(object):
    """This class represents expressions as encountered in SQL "where clauses"."""

    def __init__(self, operator, *exps):
        self.operator = operator
        self.exps = exps

    def is_boolean_expression(self):
        return True

    def evaluate_criterion(self, criterion, value):

        criterion_str = criterion.__str__()
        # replace equality operator
        criterion_str = criterion_str.replace(" = ", " == ")
        # remove prefix of arguments
        criterion_str = criterion_str.replace(":", "")
        # remove quotes arround attributes
        criterion_str = criterion_str.replace("\"", "")
        # replace "IN" operator by "in" operator
        criterion_str = criterion_str.replace(" IN ", " in ")
        # replace "IS" operator by "is" operator
        criterion_str = criterion_str.replace(" IS ", " is ")
        # replace "NOT" operator by "not" operator
        criterion_str = criterion_str.replace(" NOT ", " not ")
        # replace "NULL" operator by "None" operator
        criterion_str = criterion_str.replace("NULL", "None")
        # Format correctly lists
        criterion_str = criterion_str.replace("(", "[")
        criterion_str = criterion_str.replace(")", "]")


        # construct a dict with the values involved in the expression
        values_dict = {}

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

        for key in value.keys():
            try:
                s = LazyDictionnary(**value[value.keys().index(key)])
                values_dict[key] = s
            except:
                print("[BUG] evaluation failed: %s -> %s" % (key, value))
                return False
        # check if right value is a named argument
        expressions = []
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
        result = eval(criterion_str, values_dict)
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