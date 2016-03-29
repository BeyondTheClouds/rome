import logging

from _fixtures import *
from lib.rome.core.models import Entity
from lib.rome.core.models import global_scope

BASE = declarative_base()

from lib.rome.utils.SecondaryIndexDecorator import secondary_index_decorator

@global_scope
@secondary_index_decorator("name")
@secondary_index_decorator("specy")
class Dog(BASE, Entity):
    """Represents a dog."""

    __tablename__ = 'dogs'

    id = Column(Integer, primary_key=True)

    name = Column(String(255))
    specy = Column(String(255))
    is_barking = Column(Boolean, default=False)


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    bobby = Dog()
    bobby.name = "Bobby"
    bobby.specy = "Griffon"
    bobby.save()
