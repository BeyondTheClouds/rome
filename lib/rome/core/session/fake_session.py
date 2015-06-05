__author__ = 'jonathan'

from lib.rome.core.orm.query import Query

class ControlledExecution():

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass

class FakeSession():

    def add(self, *objs):
        for obj in objs:
            obj.save()

    def query(self, *entities, **kwargs):
        return Query(*entities, **kwargs)

    def begin(self, *args, **kwargs):
        return ControlledExecution()

    def flush(self, *args, **kwargs):
        pass


#
# def get_session(use_slave=False, **kwargs):
#     # facade = _create_facade_lazily(use_slave)
#     # return facade.get_session(**kwargs)
#
#     return FakeSession()
