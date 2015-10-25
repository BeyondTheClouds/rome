__author__ = 'jonathan'

import unittest
import logging
import time
import functools

from lib.rome.core.orm.query import Query
from test._fixtures import *

BASE = declarative_base()

from lib.rome.core.models import Entity
from lib.rome.core.models import global_scope
# from lib.rome.core.session.session import OldSession as Session
from lib.rome.core.session.session import Session as Session
from oslo.db.exception import DBDeadlock

import random

BASE = declarative_base()


@global_scope
class BankAccount(BASE, Entity):
    """Represents a bank account."""

    __tablename__ = 'bank_account'

    id = Column(Integer, primary_key=True)
    owner = Column(String)
    money = Column(Integer)



def _retry_on_deadlock(f):
    """Decorator to retry a DB API call if Deadlock was received."""
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        while True:
            try:
                return f(*args, **kwargs)
            except DBDeadlock:
                logging.warn(("Deadlock detected when running '%s': Retrying...") % (f.__name__))
                # Retry!
                time.sleep(random.uniform(0.1, 0.5))
                continue
    functools.update_wrapper(wrapped, f)
    return wrapped

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
        logging.getLogger().setLevel(logging.DEBUG)

        for i in range(1, 2):
            import threading
            import time

            # print("cleaning existing accounts")
            existing_accounts = Query(BankAccount).all()
            for each in existing_accounts:
                each.soft_delete()

            bob_account = BankAccount()
            bob_account.money = 1000
            bob_account.owner = "bob"
            bob_account.save()

            alice_account = BankAccount()
            alice_account.money = 1000
            alice_account.owner = "alice"
            alice_account.save()

            @_retry_on_deadlock
            def transfer():
                session = Session()
                with session.begin():
                    # print("executing")
                    accounts = Query(BankAccount, session=session).all()

                    bob_account = accounts[0] if accounts[0].owner is "bob" else accounts[1]
                    alice_account = accounts[0] if accounts[0].owner is "alice" else accounts[1]

                    # bob_account.money -= 100
                    # alice_account.money += 100

                    bob_account.update({"money": bob_account.money - 100})
                    alice_account.update({"money": alice_account.money + 100})

                    session.add(bob_account)

                    session.flush()
                    session.add(alice_account)

                    # bob_account.save()
                    # alice_account.save()

            a = threading.Thread(target=transfer)
            b = threading.Thread(target=transfer)
            a.start()
            b.start()

            time.sleep(1)

            existing_accounts = Query(BankAccount).all()
            for each in existing_accounts:
                print(each.money)
            print "____"


if __name__ == '__main__':
    unittest.main()

