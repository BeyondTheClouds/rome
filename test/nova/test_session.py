__author__ = 'jonathan'

import unittest
from lib.rome.core.session.session import Session as Session
from lib.rome.core.orm.query import Query as Query
from test.test_dogs import *
import _fixtures as models
import time

import lib.rome.driver.database_driver as database_driver
from lib.rome.core.models import Entity
from lib.rome.core.models import global_scope
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Index, Integer, BigInteger, Enum, String, schema
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy import orm
from sqlalchemy.sql import func
from sqlalchemy import ForeignKey, DateTime, Boolean, Text, Float
import logging
from lib.rome.core.models import get_model_class_from_name
BASE = declarative_base()

from lib.rome.utils.SecondaryIndexDecorator import secondary_index_decorator

@global_scope
class BankAccount(BASE, Entity):
    """Represents a bank account."""

    __tablename__ = 'bank_account'

    id = Column(Integer, primary_key=True)
    money = Column(Integer)


class TestSession(unittest.TestCase):

    # def test_session_instance_modification(self):
    #     logging.getLogger().setLevel(logging.DEBUG)
    #     session = Session()
    #     with session.begin():
    #         instances = session.query(models.Instance).all()
    #         instances[0].hostname += "aa"
    #         # dogs[0].update({"name": dogs[0].name + "bb"})
    #         # dogs[0].save(session=session)
    #         # dogs[0].save(session=session)
    #         # raise Exception("toto")

    # def test_session_execution(self):
    #     logging.getLogger().setLevel(logging.DEBUG)
    #     session = Session()
    #     with session.begin():
    #         dogs = session.query(Dog).all()
    #         print("dogs: %s" % (dogs))
    #         print("session_id: %s" % (session.session_id))
    #         dogs[0].name += "aa"
    #         # dogs[0].update({"name": dogs[0].name + "bb"})
    #         # dogs[0].save(session=session)
    #         # dogs[0].save(session=session)
    #         # raise Exception("toto")

    def test_concurrent_update(self):
        import threading
        import time

        def work(id):
            # logging.getLogger().setLevel(logging.DEBUG)
            # session = Session()
            # with session.begin():
            #     dogs = session.query(Dog).all()
            #     time.sleep(1)
            #     # print("dogs: %s" % (dogs))
            #     # print("session_id: %s" % (session.session_id))
            #     dogs[0].name += "bb"
            #     # dogs[0].update({"name": dogs[0].name + "bb"})
            #     # dogs[0].save(session=session)
            # time.sleep(0.02)
            # print("end")
            while True:
                print(id)
                time.sleep(1)

        a = threading.Thread(None, work(), None)
        # b = threading.Thread(None, work(), None)
        a.start()
        # b.start()


if __name__ == '__main__':

    print("cleaning existing accounts")
    existing_accounts = Query(id, BankAccount).all()
    for each in existing_accounts:
        each.soft_delete()

    new_account = BankAccount()
    new_account.money = 1000
    new_account.save()

    unittest.main()