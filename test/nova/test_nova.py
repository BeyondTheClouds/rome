__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query

if __name__ == '__main__':

    host = "paranoia-7"
    query = Query(models.Service).filter(models.Service.host==host)
    result = query.all()
    print(map(lambda x: x.host, result))
    # query = Query(models.Network).filter_by(id=1)
    # result = query.all()
    # print(result)
    pass