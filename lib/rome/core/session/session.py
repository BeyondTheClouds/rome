__author__ = 'jonathan'

from lib.rome.core.utils import merge_dicts
from lib.rome.core.utils import current_milli_time
from lib.rome.core.lazy_reference import LazyReference
from lib.rome.driver.redis.lock import ClusterLock

import logging

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
        else:
            self.session.flush()

class Session(object):

    max_duration = 300

    def __init__(self):
        self.session_id = uuid.uuid1()
        self.session_objects_add = []
        self.session_objects_delete = []
        self.session_timeout = current_milli_time() + Session.max_duration
        self.dlm = ClusterLock()

    def add(self, *objs):
        self.session_objects_add += objs

    def delete(self, *objs):
        self.session_objects_delete += objs

    def query(self, *entities, **kwargs):
        from lib.rome.core.orm.query import Query
        return Query(*entities, **merge_dicts(kwargs, {"session": self}))

    def begin(self, *args, **kwargs):
        return SessionControlledExecution(session=self)

    def flush(self, *args, **kwargs):
        logging.info("flushing session %s" % (self.session_id))
        if self.can_commit_request():
            logging.info("committing session %s" % (self.session_id))
            self.commit()

    def can_be_used(self, obj):
        if getattr(obj, "session", None) is None:
            return True
        else:
            if obj.session["session_id"] == self.session_id:
                return True
            if current_milli_time >= obj.session["session_timeout"]:
                return True
        logging.error("session %s cannot use object %s" % (self.session_id, obj))
        return False

    def can_commit_request(self):
        # processed_objects = []
        locks = []
        success = True
        for obj in self.session_objects_add + self.session_objects_delete:
            if obj.id is not None:
                lock_name = "session_lock_%s_%s" % (obj.__tablename__, obj.id)
                if self.dlm.lock(lock_name, 100):
                    locks += [lock_name]
                else:
                    success = False
                    break
                # recent_version = LazyReference(obj.__tablename__, obj.id, self.session_id, None).load()
                # if self.can_be_used(recent_version):
                #     session_object = {"session_id": str(self.session_id), "session_timeout": self.session_timeout}
                #     recent_version.update({"session": session_object})
                #     recent_version.save(force=True, session=self, no_nested_save=True, increase_version=False)
                #     processed_objects += [recent_version]
                # else:
                #     success = False
        if not success:
            logging.error("session %s encountered a conflict, aborting commit" % (self.session_id))
            for lock in locks:
                self.dlm.unlock(lock)
            # for obj in processed_objects:
            #     obj.session = None
            #     obj.save(force=True, no_nested_save=True)
        return success

    def commit(self):
        logging.info("session %s will start commit" % (self.session_id))
        for obj in self.session_objects_add:
            # obj.update({"session": None}, skip_session=True)
            obj.save(force=True)
        for obj in self.session_objects_delete:
            # obj.update({"session": None}, skip_session=True)
            obj.soft_delete(force=True)
        logging.info("session %s committed" % (self.session_id))
        self.session_objects_add = []
        self.session_objects_delete = []
