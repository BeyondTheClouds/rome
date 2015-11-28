__author__ = 'jonathan'

import threading
import Queue

class MemoizationDecorator(object):

    conditions_objects_dict = {}
    memory = {}
    insertion_lock = threading.Lock()

    def __init__(self, decorated):
        self.decorated = decorated

    def __getattr__(self, attribute_name):
        decorated_attribute = getattr(self.decorated, attribute_name)
        if hasattr(decorated_attribute, "__call__"):
            callable_object = self.FunctionWrapper(decorated_attribute, method_name=attribute_name, memory=self.memory, insertion_lock=self.insertion_lock)
            return callable_object
        return decorated_attribute

    class FunctionWrapper:

        """Class that is used to "delay" call to decorated's method."""

        def __init__(self, callable_object, method_name, memory, insertion_lock):
            self.callable_object = callable_object
            self.method_name = method_name
            self.memory = memory
            self.insertion_lock = insertion_lock

        def compute_hash(self, method_name, *args, **kwargs):
            hashv = hash("%s_%s_%s" % (method_name, args, kwargs))
            return hashv

        def __call__(self, *args, **kwargs):

            call_hash = self.compute_hash(self.method_name, args, kwargs)

            if call_hash in self.memory:
                # Increment safely the number of threads waiting for expected value
                item = self.memory[call_hash]
                should_retry = True
                item["modification_lock"].acquire()
                if not item["closed"]:
                    item["waiting_threads_count"] += 1
                    should_retry = False
                item["modification_lock"].release()
                if should_retry:
                    # memory has been destroyed by a master call, simply abort it and repeat the method.
                    return self.__call__(*args, **kwargs)
                # Wait for the expected value.
                result = item["result_queue"].get()
            else:
                # try insertion
                should_retry = True
                self.insertion_lock.acquire()
                if not call_hash in self.memory:
                    self.memory[call_hash] = {
                        "modification_lock": threading.Lock(),
                        "result_queue": Queue.Queue(),
                        "result": None,
                        "waiting_threads_count": 0,
                        "closed": False
                    }
                    should_retry = False
                self.insertion_lock.release()

                if should_retry:
                    # memory has been initialised by a quicker concurrent call, simply abort it and become a slave.
                    return self.__call__(*args, **kwargs)

                # compute the expected value and store it in a shared memory.
                result = self.callable_object(*args, **kwargs)
                self.memory[call_hash]["result"] = result

                # close safely the memory item
                self.memory[call_hash]["modification_lock"].acquire()
                self.memory[call_hash]["closed"] = True
                self.memory[call_hash]["modification_lock"].release()

                # notify paused concurrent calls that the expected value is ready to be used.
                # self.memory[call_hash]["event"].set()

                # delete the memory item
                item = self.memory[call_hash]

                # send to concurrent slave calls.
                for i in range(item["waiting_threads_count"]):
                    item["result_queue"].put(result)

                # Once there are no more slave calls, the item can be destroyed
                item["modification_lock"].acquire()
                self.insertion_lock.acquire()
                del self.memory[call_hash]
                item["modification_lock"].release()
                self.insertion_lock.release()
            return result


def memoization_decorator(func):
    def wrapper(*args, **kwargs):
        return MemoizationDecorator(func(*args, **kwargs))
    return wrapper

if __name__ == '__main__':

    import time

    class Foo(object):
        def get_magical_value(self, cpt):
            print("starting")
            time.sleep(7)
            print("ending")
            return cpt

    # obj1 = Foo()
    obj1 = MemoizationDecorator(Foo())

    def do_request():
        value = obj1.get_magical_value(42)
        print(value)

    for n in range(2):
        thread = threading.Thread(target=do_request)
        thread.start()
        time.sleep(1)
    time.sleep(3)
    for n in range(3):
        thread = threading.Thread(target=do_request)
        thread.start()
        time.sleep(1)
