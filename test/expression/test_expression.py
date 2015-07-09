import unittest

from sqlalchemy.ext.declarative import declarative_base
import test.nova._fixtures as models
from sqlalchemy.util._collections import KeyedTuple
from lib.rome.core.orm.query import or_
from lib.rome.core.orm.query import and_

BASE = declarative_base()

from lib.rome.core.expression.expression import BooleanExpression as BooleanExpression

class TestLock(unittest.TestCase):

    def test_expression_equality(self):
        """Checks on a specified attribute with operators "==" and "!="."""

        # Checks on a specified attribute with operators "==" and "!=" with integers
        expression = BooleanExpression("NORMAL", models.Network.id == 1)
        value = expression.evaluate(KeyedTuple([{"id": 1}], ["networks"]))
        self.assertTrue(value, "models.Network.id==1 with models.Network.id=1")

        expression = BooleanExpression("NORMAL", models.Network.id == 1)
        value = expression.evaluate(KeyedTuple([{"id": 2}], ["networks"]))
        self.assertFalse(value, "models.Network.id==1 with models.Network.id=2")

        expression = BooleanExpression("NORMAL", models.Network.id != 1)
        value = expression.evaluate(KeyedTuple([{"id": 1}], ["networks"]))
        self.assertFalse(value, "models.Network.id!=1 with models.Network.id=1")

        expression = BooleanExpression("NORMAL", models.Network.id != 1)
        value = expression.evaluate(KeyedTuple([{"id": 2}], ["networks"]))
        self.assertTrue(value, "models.Network.id!=1 with models.Network.id=2")

        # Checks on a specified attribute with operators "==" and "!=" with string
        expression = BooleanExpression("NORMAL", models.Network.label == "network_1")
        value = expression.evaluate(KeyedTuple([{"label": "network_1"}], ["networks"]))
        self.assertTrue(value, """models.Network.label=="network_1" with models.Network.label="network_1" """)

        expression = BooleanExpression("NORMAL", models.Network.label == "network_1")
        value = expression.evaluate(KeyedTuple([{"label": "network_2"}], ["networks"]))
        self.assertFalse(value, """models.Network.label=="network_1" with models.Network.label="network_2" """)

        expression = BooleanExpression("NORMAL", models.Network.label != "network_1")
        value = expression.evaluate(KeyedTuple([{"label": "network_1"}], ["networks"]))
        self.assertFalse(value, """models.Network.label!="network_1" with models.Network.label="network_1" """)

        expression = BooleanExpression("NORMAL", models.Network.label != "network_1")
        value = expression.evaluate(KeyedTuple([{"label": "network_2"}], ["networks"]))
        self.assertTrue(value, """models.Network.label!="network_1" with models.Network.label="network_2" """)

        # Checks on a specified attribute with operators "IS" with string
        expression = BooleanExpression("NORMAL", models.Network.label == None)
        value = expression.evaluate(KeyedTuple([{"label": None}], ["networks"]))
        self.assertTrue(value, """models.Network.label==None with models.Network.label=None """)

        expression = BooleanExpression("NORMAL", models.Network.label == None)
        value = expression.evaluate(KeyedTuple([{"label": "network_2"}], ["networks"]))
        self.assertFalse(value, """models.Network.label==None with models.Network.label="network_2" """)

        expression = BooleanExpression("NORMAL", models.Network.label != None)
        value = expression.evaluate(KeyedTuple([{"label": None}], ["networks"]))
        self.assertFalse(value, """models.Network.label!=None with models.Network.label=None """)

        expression = BooleanExpression("NORMAL", models.Network.label != None)
        value = expression.evaluate(KeyedTuple([{"label": "network_2"}], ["networks"]))
        self.assertTrue(value, """models.Network.label!=None with models.Network.label="network_2" """)

    def test_expression_dates(self):
        """Checks on a specified attribute with operators "==" and ">" and "<" with dates."""
        import datetime
        import time
        time1 = datetime.datetime.now()
        time.sleep(0.01)
        time2 = datetime.datetime.now()

        # Checks on a specified attribute with operators "==" and "!=" with integers
        expression = BooleanExpression("NORMAL", models.Network.updated_at < time2)
        value = expression.evaluate(KeyedTuple([{"updated_at": time1}], ["networks"]))
        self.assertTrue(value, "models.Network.updated_at < time2 with models.Network.id=time1")

        expression = BooleanExpression("NORMAL", models.Network.updated_at > time2)
        value = expression.evaluate(KeyedTuple([{"updated_at": time1}], ["networks"]))
        self.assertFalse(value, "models.Network.updated_at < time2 with models.Network.id=time1")

        expression = BooleanExpression("NORMAL", models.Network.updated_at < time1)
        value = expression.evaluate(KeyedTuple([{"updated_at": time2}], ["networks"]))
        self.assertFalse(value, "models.Network.updated_at < time1 with models.Network.id=time2")

        expression = BooleanExpression("NORMAL", models.Network.updated_at > time1)
        value = expression.evaluate(KeyedTuple([{"updated_at": time2}], ["networks"]))
        self.assertTrue(value, "models.Network.updated_at < time1 with models.Network.id=time2")

        expression = BooleanExpression("NORMAL", models.Network.updated_at == time1)
        value = expression.evaluate(KeyedTuple([{"updated_at": time1}], ["networks"]))
        self.assertTrue(value, "models.Network.updated_at < time1 with models.Network.id=time2")

        expression = BooleanExpression("NORMAL", models.Network.updated_at == time2)
        value = expression.evaluate(KeyedTuple([{"updated_at": time1}], ["networks"]))
        self.assertFalse(value, "models.Network.updated_at < time1 with models.Network.id=time2")

    def test_expression_contains(self):
        """Checks on a specified attribute with operators "IN"."""

        # Checks on a specified attribute with operators "==" and "!=" with integers
        expression = BooleanExpression("NORMAL", models.Network.id.in_([1, 3, 4]))
        value = expression.evaluate(KeyedTuple([{"id": 1}], ["networks"]))
        self.assertTrue(value, "models.Network.id in [1, 3, 4] with models.Network.id=1")

        expression = BooleanExpression("NORMAL", models.Network.id.in_([1, 3, 4]))
        value = expression.evaluate(KeyedTuple([{"id": 2}], ["networks"]))
        self.assertFalse(value, "models.Network.id in [1, 3, 4] with models.Network.id=2")

        # Checks on a specified attribute with operators "==" and "!=" with string
        expression = BooleanExpression("NORMAL", models.Network.label.in_(["network_1", "network_3", "network_4"]))
        value = expression.evaluate(KeyedTuple([{"label": "network_1"}], ["networks"]))
        self.assertTrue(value, """models.Network.label in ["network_1", "network_3", "network_4"] with models.Network.label="network_1" """)

        expression = BooleanExpression("NORMAL", models.Network.label.in_(["network_1", "network_3", "network_4"]))
        value = expression.evaluate(KeyedTuple([{"label": "network_2"}], ["networks"]))
        self.assertFalse(value, """models.Network.label in ["network_1", "network_3", "network_4"] with models.Network.label="network_1" """)

    def test_expression_regex(self):
        """Checks on a specified attribute with operators "REGEXP"."""

        # Checks on a specified attribute with operators "==" and "!=" with integers
        expression = BooleanExpression("NORMAL", models.Network.label.op("REGEXP")("network_3"))
        value = expression.evaluate(KeyedTuple([{"label": "network_3"}], ["networks"]))
        self.assertTrue(value, """models.Network.label REGEXP /pattern/ with models.Network.label="network_3" (1)""")

        expression = BooleanExpression("NORMAL", models.Network.label.op("REGEXP")("(network_3|network_2)"))
        value = expression.evaluate(KeyedTuple([{"label": "network_3"}], ["networks"]))
        self.assertTrue(value, """models.Network.label REGEXP /pattern/ with models.Network.label="network_3" (2)""")

        expression = BooleanExpression("NORMAL", models.Network.label.op("REGEXP")("(network_1|network_2)"))
        value = expression.evaluate(KeyedTuple([{"label": "network_3"}], ["networks"]))
        self.assertFalse(value, """models.Network.label REGEXP /pattern/ with models.Network.label="network_3" (3)""")

    def test_expression_and_or(self):
        """Checks on a specified attribute with operators "IN"."""

        # Checks several examples with "and" and "or" operators
        expression = BooleanExpression("NORMAL", or_(and_(models.Network.label != "network_3", models.Network.multi_host == True), models.Network.label == "network_3"))
        value = expression.evaluate(KeyedTuple([{"label": "network_3", "multi_host": False}], ["networks"]))
        self.assertTrue(value, "complex expression (1)")

        expression = BooleanExpression("NORMAL", or_(and_(models.Network.label != "network_3", models.Network.multi_host == True), models.Network.label == "network_3"))
        value = expression.evaluate(KeyedTuple([{"label": "network_2", "multi_host": True}], ["networks"]))
        self.assertTrue(value, "complex expression (2)")

        expression = BooleanExpression("NORMAL", or_(and_(models.Network.label != "network_3", models.Network.multi_host == True), models.Network.label == "network_3"))
        value = expression.evaluate(KeyedTuple([{"label": "network_2", "multi_host": False}], ["networks"]))
        self.assertFalse(value, "complex expression (3)")

if __name__ == '__main__':
    unittest.main()