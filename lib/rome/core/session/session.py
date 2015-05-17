__author__ = 'jonathan'

from lib.rome.core.utils import merge_dicts
import uuid

class SessionException(Exception):
    pass

class SessionControlledExecution():

    def __init__(self, session):
        self.session = session

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        if traceback:
            print(traceback)
        pass

class Session(object):

    def __init__(self):
        self.session_id = uuid.uuid1()

    def add(self, *objs):
        for obj in objs:
            obj.save()

    def query(self, *entities, **kwargs):
        import lib.rome.core.orm.query as Query
        return Query(*entities, **merge_dicts(kwargs, {"session": self}))

    def begin(self, *args, **kwargs):
        return SessionControlledExecution(session=self)

    def flush(self, *args, **kwargs):
        pass

    def is_valid(self, object):
        return True