__author__ = 'jonathan'

import unittest
import logging
import time

from lib.rome.core.orm.query import Query
from _fixtures import *

BASE = declarative_base()

from lib.rome.core.models import Entity
from lib.rome.core.models import global_scope
from lib.rome.core.session.session import Session

BASE = declarative_base()


@global_scope
class BankAccount(BASE, Entity):
    """Represents a bank account."""

    __tablename__ = 'bank_account'

    id = Column(Integer, primary_key=True)
    money = Column(Integer)


cpt = 0

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

        print("cleaning existing accounts")
        existing_accounts = Query(BankAccount).all()
        for each in existing_accounts:
            each.soft_delete()

        bob_account = BankAccount()
        bob_account.money = 1000
        bob_account.save()

        alice_account = BankAccount()
        alice_account.money = 1000
        alice_account.save()

        def transfer():
            session = Session()
            with session.begin():

                accounts = Query(BankAccount, session=session).all()

                global cpt
                cpt += 1

                accounts[0].money -= 100
                accounts[1].money += 100

                # while(cpt < 2):
                #     time.sleep(0.01)

                accounts[0].save()
                accounts[1].save()

        a = threading.Thread(target=transfer)
        b = threading.Thread(target=transfer)
        a.start()
        b.start()

        time.sleep(2)

        existing_accounts = Query(BankAccount).all()
        for each in existing_accounts:
            print(each.money)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()

