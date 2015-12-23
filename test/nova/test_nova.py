__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    instance_uuid = "5a73a745-01ab-4ae2-a95c-ce47e17d9f21"
    result = Query(models.InstanceExtra).filter_by(instance_uuid=instance_uuid).all()

    print(len(result))

    # if not result:
    #     raise Exception()

    # return result

