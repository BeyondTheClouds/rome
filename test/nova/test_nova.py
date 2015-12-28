__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query
import logging

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    instance_uuid = "e114f2ae-007b-4c51-bda5-f120119ea732"
    host = "hercule-2"
    topic = "conductor"
    # result = Query(models.Service).filter(models.Service.host==host).filter(models.Service.topic==topic).all()
    result = Query(models.InstanceExtra).filter_by(instance_uuid=instance_uuid).all()
    result = Query(models.InstanceExtra).filter_by(instance_uuid=instance_uuid).all()
    result = Query(models.InstanceExtra).filter_by(instance_uuid=instance_uuid).all()
    result = Query(models.InstanceExtra).filter_by(instance_uuid=instance_uuid).all()
    result = Query(models.InstanceExtra).filter_by(instance_uuid=instance_uuid).all()
    result = Query(models.InstanceExtra).filter_by(instance_uuid=instance_uuid).all()

    print(len(result))

