__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import collections
import logging

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    instance_uuid = "2c4dcaf1-640f-4db4-89da-8e4976e4bd57"
    # instance_uuid = "e66568f4-228d-4836-9436-374201fc9d61"

    result = Query(models.InstanceSystemMetadata).\
                    filter(models.InstanceSystemMetadata.instance_uuid==instance_uuid).\
                    all()

    for system_metadata in result:
        print("%s => %s" % (system_metadata.key, system_metadata.value))

    print(len(result))

    # if not result:
    #     raise Exception()

    # return result

