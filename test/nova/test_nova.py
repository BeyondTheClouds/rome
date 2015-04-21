__author__ = 'jonathan'

import _fixtures as models
from lib.rome.core.orm.query import Query

if __name__ == '__main__':

    query = Query(models.Network).filter(models.Network.id==1)
    result = query.all()
    print(result)
    pass