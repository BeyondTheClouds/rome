"""STRING module.

This module contains functions, classes and mix-in that are used for the
simplification of objects, before storing them into the JSON database.

"""

import uuid
from lib.rome.core.dataformat.json import Encoder as JsonEncoder
from lib.rome.core.dataformat.json import Decoder as JsonDecoder
from lib.rome.core.dataformat.json import is_dict_and_has_key

class Encoder(object):
    """A class that is in charge of converting python objects (basic types,
    dictionnaries, novabase objects, ...) to a representation that can
    be stored in database."""

    def __init__(self, request_uuid=uuid.uuid1()):
        self.json_encoder = JsonEncoder(request_uuid)
        self.simple_cache = self.json_encoder.simple_cache
        self.complex_cache = self.json_encoder.complex_cache
        self.target_cache = self.json_encoder.target_cache

    def reset(self):
        self.json_encoder.reset()

    def simplify(self, obj):
        prefix = None
        is_dict = isinstance(obj, dict)
        is_list = isinstance(obj, list)
        if is_dict_and_has_key(obj, "simplify_strategy"):
            if obj['simplify_strategy'] == 'datetime':
                prefix = "datetime"
            if obj['simplify_strategy'] == 'ipnetwork':
                prefix = "ipnetwork"
            if obj['simplify_strategy'] == 'novabase':
                prefix = "novabase"
        elif is_list:
            prefix = "list"
        elif is_dict and obj.has_key("novabase_classname"):
            prefix = "novabase"
        elif is_dict and obj.has_key("_metadata_novabase_classname"):
            prefix = "novabase"
        elif is_dict:
            prefix = "dict"
        result = self.json_encoder.simplify(obj)
        if prefix is not None:
            return "%s{separator}%s" % (prefix, result)
        else:
            return result

class Decoder(object):
    """Class that translate an object containing values taken from database
    into an object containing values understandable by services composing
    Nova."""

    def __init__(self, request_uuid=uuid.uuid1()):
        self.json_decoder = JsonDecoder(request_uuid)
        self.cache = self.json_decoder.cache

    def desimplify(self, obj):
        is_dict = isinstance(obj, dict)
        is_list = isinstance(obj, list)
        is_tuple = isinstance(obj, tuple)
        if is_list:
            result = []
            for item in obj:
                result += [self.desimplify(item)]
        elif is_tuple:
            result = map(lambda x: self.desimplify(x), obj)
        elif is_dict and obj.has_key("novabase_classname"):
            result = self.json_decoder.novabase_desimplify(obj)
        elif is_dict and obj.has_key("_metadata_novabase_classname"):
            result = self.json_decoder.novabase_desimplify(obj)
        elif is_dict:
            result = {}
            for item in obj:
                result[item] = self.desimplify(obj[item])
        else:
            if (isinstance(obj, str) or isinstance(obj, unicode) )and "{separator}" in obj:
                tabs = obj.split("{separator}")
                if len(tabs) == 2:
                    string_strategy = tabs[0]
                    value = tabs[1]
                    obj = eval(value)
                    if "datetime" in string_strategy:
                        result = self.json_decoder.datetime_desimplify(obj)
                    elif "ipnetwork" in string_strategy:
                        result = self.json_decoder.ipnetwork_desimplify(obj)
                    elif "novabase" in string_strategy:
                        result = self.json_decoder.novabase_desimplify(obj)
                    elif "list" in string_strategy:
                        result = self.json_decoder.desimplify(obj)
                    elif "dict" in string_strategy:
                        result = self.json_decoder.desimplify(obj)
                    else:
                        result = self.json_decoder.desimplify(obj)
            else:
                result = self.json_decoder.desimplify(obj)
        return result
