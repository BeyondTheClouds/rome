# encoding=UTF8

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Unit tests for the DB API."""

import copy
import datetime
import uuid as stdlib_uuid

import iso8601
import mock
import netaddr
from oslo_config import cfg
from oslo_db import api as oslo_db_api
from oslo_db import exception as db_exc
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import test_base
from oslo_db.sqlalchemy import update_match
from oslo_db.sqlalchemy import utils as sqlalchemyutils
from oslo_serialization import jsonutils
from oslo_utils import fixture as utils_fixture
from oslo_utils import timeutils
from oslo_utils import uuidutils
import six
from six.moves import range
from sqlalchemy import Column
from sqlalchemy.dialects import sqlite
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy.orm import query
from sqlalchemy import sql
from sqlalchemy import Table

from nova import block_device
from nova.compute import arch
from nova.compute import task_states
from nova.compute import vm_states
from nova import context
from nova import db
# from nova.db.sqlalchemy import api as sqlalchemy_api
# from nova.db.sqlalchemy import models
from nova.db.discovery import api as sqlalchemy_api
from nova.db.discovery import models
from nova.db.sqlalchemy import types as col_types
from nova.db.sqlalchemy import utils as db_utils
from nova import exception
from nova import objects
from nova.objects import fields
from nova import quota
from nova import test
from nova.tests.unit import matchers
from nova.tests import uuidsentinel
from nova import utils

from lib.rome.core.session.session import Session as RomeSession

CONF = cfg.CONF
CONF.import_opt('reserved_host_memory_mb', 'nova.compute.resource_tracker')
CONF.import_opt('reserved_host_disk_mb', 'nova.compute.resource_tracker')

get_engine = sqlalchemy_api.get_engine

from nova.context import RequestContext
import functools

def wrapp_with_session(f):
    """Decorator to use a writer db context manager.

    The db context manager will be picked from the RequestContext.

    Wrapped function must have a RequestContext in the arguments.
    """
    @functools.wraps(f)
    def wrapped(context, *args, **kwargs):
        ctxt_mgr = RomeTransactionContext()
        if context.session is None:
            context.session = RomeSession()
        with context.session.begin():
            return f(context, *args, **kwargs)
    return wrapped

@wrapp_with_session
def service_create(context, values):
    print("creating a service with following properties: %s" % (values))
    service_ref = models.Service()
    service_ref.update(values)
    if not CONF.enable_new_services:
        service_ref.disabled = True
    try:
        service_ref.save(context.session)
    except db_exc.DBDuplicateEntry as e:
        if 'binary' in e.columns:
            raise exception.ServiceBinaryExists(host=values.get('host'),
                        binary=values.get('binary'))
        raise exception.ServiceTopicExists(host=values.get('host'),
                        topic=values.get('topic'))
    return service_ref



class RomeTransactionContext():
    def __init__(self, mode=None):
        if not mode:
            self.writer = RomeTransactionContext(mode="writer")
            self.reader = RomeTransactionContext(mode="reader")
        pass

class RomeRequestContext(object):
    def __init__(self, *args, **kwargs):
        self.ctxt = RequestContext(*args, **kwargs)
        self.session = RomeSession()

if __name__ == "__main__":
    import logging
    logging.getLogger().setLevel(logging.DEBUG)
    # ctxt = context.get_admin_context()
    ctxt = RomeRequestContext(user_id=None,
                          project_id=None,
                          is_admin=True,
                          read_deleted="no",
                          overwrite=False)
    service_dict = dict(host='host1', binary='nova-compute',
                            topic=CONF.compute_topic, report_count=1,
                            disabled=False)
    service_create(ctxt, service_dict)