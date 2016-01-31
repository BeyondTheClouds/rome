from lib.rome.core.models import Entity, Base, global_scope
from sqlalchemy import Column , Integer, String
from lib.rome.core.orm.query import Query
from sqlalchemy import orm

@global_scope
class Specy(Base, Entity):
    """Represents a specy."""

    __tablename__ = 'species'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))

@global_scope
class Dog(Base, Entity):
    """Represents a dog."""

    __tablename__ = 'dogs'

    id = Column(Integer, primary_key=True)

    name = Column(String(255))
    specy_id = Column(Integer)
    specy = orm.relationship(Specy, backref="dogs",
                 foreign_keys=specy_id,
                 primaryjoin='Dog.specy_id == Specy.id')

if __name__ == '__main__':

    dogs_names = ["rintintin", "rantanplan", "bobby"]
    species_names = ["griffon", "beaggle", "labrador", "cocker"]
    if Query(Specy).count() == 0:
        for specy_name in species_names:
            specy = Specy()
            specy.name = specy_name
            specy.save()

    if Query(Dog).count() == 0:
        for specy in Query(Specy).all():
            for dog_name in dogs_names:
                dog = Dog()
                dog.name = dog_name
                dog.specy_id = specy.id
                dog.save()

    # query = Query(Dog).join(Specy, Dog.specy_id==Specy.id)
    # for row in query.all():
    #     print(row[0].specy.dogs)
    # print(query.count())
    print(Query(Dog).first().specy.dogs[0].specy.dogs[1].specy)

    # query = Query(Dog.id, Dog.name, Specy.id, Specy.name).join(Specy, Dog.specy_id==Specy.id)
    # for row in query.all():
    #     print(row[0])
    # print(query.count())


