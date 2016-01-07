__author__ = 'jonathan'
import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

from oslo.utils import timeutils
import logging
import six
import datetime
import pytz

def get_session(use_slave=False, **kwargs):
    # return FakeSession()
    return RomeSession()
    # return OldRomeSession()


def model_query(context, *args, **kwargs):
    # base_model = kwargs["base_model"]
    # models = args
    return RomeQuery(*args, **kwargs)

def _handle_objects_related_type_conversions(values):
    """Make sure that certain things in values (which may have come from
    an objects.instance.Instance object) are in suitable form for the
    database.
    """
    # NOTE(danms): Make sure IP addresses are passed as strings to
    # the database engine
    for key in ('access_ip_v4', 'access_ip_v6'):
        if key in values and values[key] is not None:
            values[key] = str(values[key])

    datetime_keys = ('created_at', 'deleted_at', 'updated_at',
                     'launched_at', 'terminated_at', 'scheduled_at')
    convert_objects_related_datetimes(values, *datetime_keys)

def convert_objects_related_datetimes(values, *datetime_keys):
    for key in datetime_keys:
        if key in values and values[key]:
            if isinstance(values[key], six.string_types):
                values[key] = timeutils.parse_strtime(values[key])
            # NOTE(danms): Strip UTC timezones from datetimes, since they're
            # stored that way in the database
            values[key] = values[key].replace(tzinfo=None)
    return values

def compute_node_get(context, compute_id):
    return _compute_node_get(context, compute_id)


def _compute_node_get(context, compute_id, session=None):
    result = model_query(context, models.ComputeNode, session=session).\
            filter_by(id=compute_id).\
            first()

    if not result:
        Exception()

    return result

def compute_node_update(context, compute_id, values):
    """Updates the ComputeNode record with the most recent data."""

    session = get_session()
    with session.begin():
        compute_ref = _compute_node_get(context, compute_id, session=session)
        # Always update this, even if there's going to be no other
        # changes in data.  This ensures that we invalidate the
        # scheduler cache of compute node data in case of races.
        values['updated_at'] = timeutils.utcnow()
        datetime_keys = ('created_at', 'deleted_at', 'updated_at')
        convert_objects_related_datetimes(values, *datetime_keys)
        compute_ref.update(values)
        # TODO (Jonathan): add a "session.add" to ease the session management :)
        session.add(compute_ref)


    return compute_ref

class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id

if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("project1", "user1")
    some_date = timeutils.utcnow()
    print(some_date)
    print("%s" % (some_date))

    date_str = "Oct 25 2015 21:37:46"
    date_str = "2015-10-25 23:04:23.452603"
    date_str = "2015-10-25T21:15:16.000000"
    result = None
    timezone = "UTC"
    result = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
    result.strftime('%Y-%m-%dT%H:%M:%S.%f')
    if timezone == "UTC":
        result = pytz.utc.localize(result)
    print(result)
    print("%s" % (result))
    print("%s" % (result.isoformat()))


    # values = {"created_at": some_date}
    # _handle_objects_related_type_conversions(values)
    # print(values)
