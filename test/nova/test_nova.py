__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import logging

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    instance_uuid = "00cb8937-df46-4706-be16-17b7c65faa8c"
    result = Query(models.InstanceSystemMetadata).filter(models.InstanceSystemMetadata.instance_uuid==instance_uuid).all()

    print(len(result))

