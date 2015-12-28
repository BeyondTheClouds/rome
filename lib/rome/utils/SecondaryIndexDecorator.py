__author__ = 'jonathan'

SECONDARY_INDEXES = {}

class SecondaryIndexDecorator(object):

    def __init__(self, attribute):
        self.attribute = attribute

    def __call__(self, model_class):
        # <old implementation>
        current_secondary_indexes = getattr(model_class, "_secondary_indexes", [])
        setattr(model_class, "_secondary_indexes", current_secondary_indexes + [self.attribute])
        # </old implementation>

        # <new implementation>
        # tablename = model_class.__tablename__
        # current_secondary_indexes = SECONDARY_INDEXES[tablename] if tablename in SECONDARY_INDEXES else []
        # current_secondary_indexes += [self.attribute]
        # SECONDARY_INDEXES[tablename] = current_secondary_indexes
        # current_secondary_indexes = getattr(model_class, "_secondary_indexes", [])
        # setattr(model_class, "_secondary_indexes", current_secondary_indexes + [self.attribute])
        # </new implementation>
        return model_class

def secondary_index_decorator(attribute):
    return SecondaryIndexDecorator(attribute)

if __name__ == '__main__':
    pass