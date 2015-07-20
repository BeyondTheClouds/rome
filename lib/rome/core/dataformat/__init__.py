__author__ = 'jonathan'

from lib.rome.conf.Configuration import get_config
import uuid

def get_encoder(request_uuid=uuid.uuid1()):
    # if get_config().backend() == "cassandra":
    #     from lib.rome.core.dataformat.string import Encoder
    # else:
    from lib.rome.core.dataformat.json import Encoder
    return Encoder(request_uuid)

def get_decoder(request_uuid=uuid.uuid1()):
    # if get_config().backend() == "cassandra":
    #     from lib.rome.core.dataformat.string import Decoder
    # else:
    from lib.rome.core.dataformat.json import Decoder
    return Decoder(request_uuid)
