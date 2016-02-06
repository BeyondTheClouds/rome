import logging

import six
import sqlalchemy.orm as sa_orm
import sqlalchemy.sql as sa_sql

import test.glance.models as models
from lib.rome.core.orm.query import Query
from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession
# from oslo.utils import timeutils
from lib.rome.core.utils import timeutils
from sqlalchemy.sql.expression import desc
from lib.rome.core.orm.query import or_
from lib.rome.core.orm.query import and_
import sqlalchemy

LOG = logging.getLogger()

STATUSES = ['active', 'saving', 'queued', 'killed', 'pending_delete',
            'deleted', 'deactivated']

def _update_values(image_ref, values):
    for k in values:
        if getattr(image_ref, k) != values[k]:
            setattr(image_ref, k, values[k])

def _drop_protected_attrs(model_class, values):
    """
    Removed protected attributes from values dictionary using the models
    __protected_attributes__ field.
    """
    for attr in model_class.__protected_attributes__:
        if attr in values:
            del values[attr]

def get_session(use_slave=False, **kwargs):
    # return FakeSession()
    return RomeSession()
    # return OldRomeSession()


def model_query(context, *args, **kwargs):
    # base_model = kwargs["base_model"]
    # models = args
    return RomeQuery(*args, **kwargs)


def is_image_mutable(context, image):
    """Return True if the image is mutable in this context."""
    # Is admin == image mutable
    if context.is_admin:
        return True

    # No owner == image not mutable
    if image['owner'] is None or context.owner is None:
        return False

    # Image only mutable by its owner
    return image['owner'] == context.owner

def _check_mutate_authorization(context, image_ref):
    if not is_image_mutable(context, image_ref):
        # LOG.warn(_LW("Attempted to modify image user did not own."))
        msg = "You do not own this image"
        if image_ref.is_public:
            exc_class = Exception("exception.ForbiddenPublicImage")
        else:
            exc_class = Exception("exception.Forbidden")

        raise exc_class(msg)

def _validate_image(values, mandatory_status=True):
    """
    Validates the incoming data and raises a Invalid exception
    if anything is out of order.
    :param values: Mapping of image metadata to check
    :param mandatory_status: Whether to validate status from values
    """

    if mandatory_status:
        status = values.get('status')
        if not status:
            msg = "Image status is required."
            raise Exception("exception.Invalid(msg)")

        if status not in STATUSES:
            msg = "Invalid image status '%s' for image." % status
            raise Exception("exception.Invalid(msg)")

    # # validate integer values to eliminate DBError on save
    # utils.validate_mysql_int(min_disk=values.get('min_disk'),
    #                          min_ram=values.get('min_ram'))

    return values



def _image_update(context, values, image_id, purge_props=False,
                  from_state=None):
    """
    Used internally by image_create and image_update
    :param context: Request context
    :param values: A dict of attributes to set
    :param image_id: If None, create the image, otherwise, find and update it
    """

    # NOTE(jbresnah) values is altered in this so a copy is needed
    values = values.copy()

    session = get_session()
    with session.begin():

        # Remove the properties passed in the values mapping. We
        # handle properties separately from base image attributes,
        # and leaving properties in the values mapping will cause
        # a SQLAlchemy model error because SQLAlchemy expects the
        # properties attribute of an Image model to be a list and
        # not a dict.
        properties = values.pop('properties', {})

        location_data = values.pop('locations', None)

        new_status = values.get('status', None)
        if image_id:
            image_ref = _image_get(context, image_id, session=session)
            session.add(image_ref)
            current = image_ref.status
            # Perform authorization check
            _check_mutate_authorization(context, image_ref)
        else:
            if values.get('size') is not None:
                values['size'] = int(values['size'])

            if 'min_ram' in values:
                values['min_ram'] = int(values['min_ram'] or 0)

            if 'min_disk' in values:
                values['min_disk'] = int(values['min_disk'] or 0)

            values['is_public'] = bool(values.get('is_public', False))
            values['protected'] = bool(values.get('protected', False))
            image_ref = models.Image()
            session.add(image_ref)

        # Need to canonicalize ownership
        if 'owner' in values and not values['owner']:
            values['owner'] = None

        if image_id:
            # Don't drop created_at if we're passing it in...
            _drop_protected_attrs(models.Image, values)
            # NOTE(iccha-sethi): updated_at must be explicitly set in case
            #                   only ImageProperty table was modifited
            values['updated_at'] = timeutils.utcnow()

        if image_id:
            query = session.query(models.Image).filter_by(id=image_id)
            if from_state:
                query = query.filter_by(status=from_state)

            mandatory_status = True if new_status else False
            _validate_image(values, mandatory_status=mandatory_status)

            # Validate fields for Images table. This is similar to what is done
            # for the query result update except that we need to do it prior
            # in this case.
            # TODO(dosaboy): replace this with a dict comprehension once py26
            #                support is deprecated.
            keys = values.keys()
            for k in keys:
                if k not in image_ref.to_dict():
                    del values[k]
            updated = query.update(values, synchronize_session='fetch')

            if not updated:
                # msg = ('cannot transition from %(current)s to '
                #          '%(next)s in update (wanted '
                #          'from_state=%(from)s)' %
                #        {'current': current, 'next': new_status,
                #         'from': from_state})
                raise Exception("exception.Conflict(msg)")

            image_ref = _image_get(context, image_id, session=session)
        else:
            image_ref.update(values)
            # Validate the attributes before we go any further. From my
            # investigation, the @validates decorator does not validate
            # on new records, only on existing records, which is, well,
            # idiotic.
            values = _validate_image(image_ref.to_dict())
            _update_values(image_ref, values)

            try:
                image_ref.save(session=session)
            except:
                raise Exception("""exception.Duplicate("Image ID %s already exists!" % values['id'])""")

        _set_properties_for_image(context, image_ref, properties, purge_props,
                                  session)

        if location_data:
            _image_locations_set(context, image_ref.id, location_data,
                                 session=session)
    # TODO: manually flush session
    session.flush()
    return image_get(context, image_ref.id)

def _image_property_update(context, prop_ref, values, session=None):
    """
    Used internally by image_property_create and image_property_update.
    """
    _drop_protected_attrs(models.ImageProperty, values)
    values["deleted"] = False
    prop_ref.update(values)
    prop_ref.save(session=session)
    return prop_ref

def image_property_create(context, values, session=None):
    """Create an ImageProperty object."""
    prop_ref = models.ImageProperty()
    prop = _image_property_update(context, prop_ref, values, session=session)
    return prop.to_dict()

def image_property_delete(context, prop_ref, image_ref, session=None):
    """
    Used internally by image_property_create and image_property_update.
    """
    session = session or get_session()
    prop = session.query(models.ImageProperty).filter_by(image_id=image_ref,
                                                         name=prop_ref).one()
    prop.delete(session=session)
    return prop

def _set_properties_for_image(context, image_ref, properties,
                              purge_props=False, session=None):
    """
    Create or update a set of image_properties for a given image
    :param context: Request context
    :param image_ref: An Image object
    :param properties: A dict of properties to set
    :param session: A SQLAlchemy session to use (if present)
    """
    orig_properties = {}
    for prop_ref in image_ref.properties:
        orig_properties[prop_ref.name] = prop_ref

    for name, value in six.iteritems(properties):
        prop_values = {'image_id': image_ref.id,
                       'name': name,
                       'value': value}
        if name in orig_properties:
            prop_ref = orig_properties[name]
            _image_property_update(context, prop_ref, prop_values,
                                   session=session)
        else:
            image_property_create(context, prop_values, session=session)

    if purge_props:
        for key in orig_properties.keys():
            if key not in properties:
                prop_ref = orig_properties[key]
                image_property_delete(context, prop_ref.name,
                                      image_ref.id, session=session)

def _normalize_locations(context, image, force_show_deleted=False):
    """
    Generate suitable dictionary list for locations field of image.
    We don't need to set other data fields of location record which return
    from image query.
    """

    if image['status'] == 'deactivated' and not context.is_admin:
        # Locations are not returned for a deactivated image for non-admin user
        image['locations'] = []
        return image

    if force_show_deleted:
        locations = image['locations']
    else:
        locations = filter(lambda x: not x.deleted, image['locations'])
    image['locations'] = [{'id': loc['id'],
                           'url': loc['value'],
                           'metadata': loc['meta_data'],
                           'status': loc['status']}
                          for loc in locations]
    return image


def image_get(context, image_id, session=None, force_show_deleted=False):
    image = _image_get(context, image_id, session=session,
                       force_show_deleted=force_show_deleted)
    image = _normalize_locations(context, image.to_dict(),
                                 force_show_deleted=force_show_deleted)
    return image

def image_location_delete(context, image_id, location_id, status,
                          delete_time=None, session=None):
    if status not in ('deleted', 'pending_delete'):
        # msg = _("The status of deleted image location can only be set to "
        #         "'pending_delete' or 'deleted'")
        raise Exception("exception.Invalid(msg)")

    try:
        session = session or get_session()
        location_ref = session.query(models.ImageLocation).filter_by(
            id=location_id).filter_by(image_id=image_id).one()

        delete_time = delete_time or timeutils.utcnow()

        location_ref.update({"deleted": True,
                             "status": status,
                             "updated_at": delete_time,
                             "deleted_at": delete_time})
        location_ref.save(session=session)
    except sa_orm.exc.NoResultFound:
        # msg = (_("No location found with ID %(loc)s from image %(img)s") %
        #        dict(loc=location_id, img=image_id))
        # LOG.warn(msg)
        raise Exception("exception.NotFound(msg)")


def image_location_add(context, image_id, location, session=None):
    deleted = location['status'] in ('deleted', 'pending_delete')
    delete_time = timeutils.utcnow() if deleted else None
    location_ref = models.ImageLocation(image_id=image_id,
                                        value=location['url'],
                                        meta_data=location['metadata'],
                                        status=location['status'],
                                        deleted=deleted,
                                        deleted_at=delete_time)
    session = session or get_session()
    location_ref.save(session=session)

def image_location_update(context, image_id, location, session=None):
    loc_id = location.get('id')
    if loc_id is None:
        # msg = _("The location data has an invalid ID: %d") % loc_id
        raise Exception("exception.Invalid(msg)")

    try:
        session = session or get_session()
        location_ref = session.query(models.ImageLocation).filter_by(
            id=loc_id).filter_by(image_id=image_id).one()

        deleted = location['status'] in ('deleted', 'pending_delete')
        updated_time = timeutils.utcnow()
        delete_time = updated_time if deleted else None

        location_ref.update({"value": location['url'],
                             "meta_data": location['metadata'],
                             "status": location['status'],
                             "deleted": deleted,
                             "updated_at": updated_time,
                             "deleted_at": delete_time})
        location_ref.save(session=session)
    except sa_orm.exc.NoResultFound:
        # msg = (_("No location found with ID %(loc)s from image %(img)s") %
        #        dict(loc=loc_id, img=image_id))
        # LOG.warn(msg)
        raise Exception("exception.NotFound(msg)")

def _image_locations_set(context, image_id, locations, session=None):
    # NOTE(zhiyan): 1. Remove records from DB for deleted locations
    session = session or get_session()
    query = session.query(models.ImageLocation).filter_by(
        image_id=image_id).filter_by(
            deleted=False).filter(~models.ImageLocation.id.in_(
                [loc['id']
                 for loc in locations
                 if loc.get('id')]))
    for loc_id in [loc_ref.id for loc_ref in query.all()]:
        image_location_delete(context, image_id, loc_id, 'deleted',
                              session=session)

    # NOTE(zhiyan): 2. Adding or update locations
    for loc in locations:
        if loc.get('id') is None:
            image_location_add(context, image_id, loc, session=session)
        else:
            image_location_update(context, image_id, loc, session=session)

def _check_image_id(image_id):
    """
    check if the given image id is valid before executing operations. For
    now, we only check its length. The original purpose of this method is
    wrapping the different behaviors between MySql and DB2 when the image id
    length is longer than the defined length in database model.
    :param image_id: The id of the image we want to check
    :return: Raise NoFound exception if given image id is invalid
    """
    if (image_id and
       len(str(image_id)) > models.Image.id.property.columns[0].type.length):
        raise Exception("exception.ImageNotFound()")

def _image_member_find(context, session, image_id=None,
                       member=None, status=None, include_deleted=False):
    query = session.query(models.ImageMember)
    if not include_deleted:
        query = query.filter_by(deleted=False)

    if not context.is_admin:
        query = query.join(models.Image)
        filters = [
            models.Image.owner == context.owner,
            models.ImageMember.member == context.owner,
        ]
        query = query.filter(or_(*filters))

    if image_id is not None:
        query = query.filter(models.ImageMember.image_id == image_id)
    if member is not None:
        query = query.filter(models.ImageMember.member == member)
    if status is not None:
        query = query.filter(models.ImageMember.status == status)

    return query.all()

def _image_member_format(member_ref):
    """Format a member ref for consumption outside of this module."""
    return {
        'id': member_ref['id'],
        'image_id': member_ref['image_id'],
        'member': member_ref['member'],
        'can_share': member_ref['can_share'],
        'status': member_ref['status'],
        'created_at': member_ref['created_at'],
        'updated_at': member_ref['updated_at']
    }

def image_member_find(context, image_id=None, member=None,
                      status=None, include_deleted=False):
    """Find all members that meet the given criteria.
    Note, currently include_deleted should be true only when create a new
    image membership, as there may be a deleted image membership between
    the same image and tenant, the membership will be reused in this case.
    It should be false in other cases.
    :param image_id: identifier of image entity
    :param member: tenant to which membership has been granted
    :include_deleted: A boolean indicating whether the result should include
                      the deleted record of image member
    """
    session = get_session()
    members = _image_member_find(context, session, image_id,
                                 member, status, include_deleted)
    return [_image_member_format(m) for m in members]

def is_image_visible(context, image, status=None):
    """Return True if the image is visible in this context."""
    # Is admin == image visible
    if context.is_admin:
        return True

    # No owner == image visible
    if image['owner'] is None:
        return True

    # Image is_public == image visible
    if image['is_public']:
        return True

    # Perform tests based on whether we have an owner
    if context.owner is not None:
        if context.owner == image['owner']:
            return True

        # Figure out if this image is shared with that tenant
        members = image_member_find(context,
                                    image_id=image['id'],
                                    member=context.owner,
                                    status=status)
        if members:
            return True

    # Private image
    return False

def _image_get(context, image_id, session=None, force_show_deleted=False):
    """Get an image or raise if it does not exist."""
    _check_image_id(image_id)
    session = session or get_session()

    try:
        query = session.query(models.Image).options(
            sa_orm.joinedload(models.Image.properties)).options(
                sa_orm.joinedload(
                    models.Image.locations)).filter_by(id=image_id)

        # filter out deleted images if context disallows it
        if not force_show_deleted and not context.can_see_deleted:
            query = query.filter_by(deleted=False)

        image = query.first()

    except sa_orm.exc.NoResultFound:
        msg = "No image found with ID %s" % image_id
        LOG.debug(msg)
        raise Exception("exception.ImageNotFound(msg)")

    # Make sure they can look at it
    if not is_image_visible(context, image):
        msg = "Forbidding request, image %s not visible" % image_id
        LOG.debug(msg)
        raise Exception("exception.Forbidden(msg)")

    return image

def image_create(context, values):
    """Create an image from the values dictionary."""
    return _image_update(context, values, None, purge_props=False)

def _make_conditions_from_filters(filters, is_public=None):
    # NOTE(venkatesh) make copy of the filters are to be altered in this
    # method.
    filters = filters.copy()

    image_conditions = []
    prop_conditions = []
    tag_conditions = []

    if is_public is not None:
        image_conditions.append(models.Image.is_public == is_public)

    if 'checksum' in filters:
        checksum = filters.pop('checksum')
        image_conditions.append(models.Image.checksum == checksum)

    if 'is_public' in filters:
        key = 'is_public'
        value = filters.pop('is_public')
        prop_filters = _make_image_property_condition(key=key, value=value)
        prop_conditions.append(prop_filters)

    for (k, v) in filters.pop('properties', {}).items():
        prop_filters = _make_image_property_condition(key=k, value=v)
        prop_conditions.append(prop_filters)

    if 'changes-since' in filters:
        # normalize timestamp to UTC, as sqlalchemy doesn't appear to
        # respect timezone offsets
        changes_since = timeutils.normalize_time(filters.pop('changes-since'))
        image_conditions.append(models.Image.updated_at > changes_since)

    if 'deleted' in filters:
        deleted_filter = filters.pop('deleted')
        image_conditions.append(models.Image.deleted == deleted_filter)
        # TODO(bcwaldon): handle this logic in registry server
        if not deleted_filter:
            image_statuses = [s for s in STATUSES if s != 'killed']
            image_conditions.append(models.Image.status.in_(image_statuses))

    if 'tags' in filters:
        tags = filters.pop('tags')
        for tag in tags:
            tag_filters = [models.ImageTag.deleted == False]
            tag_filters.extend([models.ImageTag.value == tag])
            tag_conditions.append(tag_filters)

    filters = {k: v for k, v in filters.items() if v is not None}

    for (k, v) in filters.items():
        key = k
        if k.endswith('_min') or k.endswith('_max'):
            key = key[0:-4]
            try:
                v = int(filters.pop(k))
            except ValueError:
                # msg = _("Unable to filter on a range "
                #         "with a non-numeric value.")
                raise Exception("exception.InvalidFilterRangeValue(msg)")

            if k.endswith('_min'):
                image_conditions.append(getattr(models.Image, key) >= v)
            if k.endswith('_max'):
                image_conditions.append(getattr(models.Image, key) <= v)

    for (k, v) in filters.items():
        value = filters.pop(k)
        if hasattr(models.Image, k):
            image_conditions.append(getattr(models.Image, k) == value)
        else:
            prop_filters = _make_image_property_condition(key=k, value=value)
            prop_conditions.append(prop_filters)

    return image_conditions, prop_conditions, tag_conditions

def _make_image_property_condition(key, value):
    prop_filters = [models.ImageProperty.deleted == False]
    prop_filters.extend([models.ImageProperty.name == key])
    prop_filters.extend([models.ImageProperty.value == value])
    return prop_filters


def _select_images_query(context, image_conditions, admin_as_user,
                         member_status, visibility):
    session = get_session()

    img_conditional_clause = and_(*image_conditions)

    regular_user = (not context.is_admin) or admin_as_user

    query_member = session.query(models.Image).join(
        models.ImageMember).filter(img_conditional_clause)
    if regular_user:
        member_filters = [models.ImageMember.deleted == False]
        if context.owner is not None:
            member_filters.extend([models.ImageMember.member == context.owner])
            if member_status != 'all':
                member_filters.extend([
                    models.ImageMember.status == member_status])
        query_member = query_member.filter(and_(*member_filters))

    # NOTE(venkatesh) if the 'visibility' is set to 'shared', we just
    # query the image members table. No union is required.
    if visibility is not None and visibility == 'shared':
        return query_member

    query_image = session.query(models.Image).filter(img_conditional_clause)
    if regular_user:
        query_image = query_image.filter(models.Image.is_public == True)
        query_image_owner = None
        if context.owner is not None:
            query_image_owner = session.query(models.Image).filter(
                models.Image.owner == context.owner).filter(
                    img_conditional_clause)
        if query_image_owner is not None:
            query = query_image.union(query_image_owner, query_member)
        else:
            query = query_image.union(query_member)
        return query
    else:
        # Admin user
        return query_image

def _paginate_query(query, model, limit, sort_keys, marker=None,
                    sort_dir=None, sort_dirs=None):
    """Returns a query with sorting / pagination criteria added.
    Pagination works by requiring a unique sort_key, specified by sort_keys.
    (If sort_keys is not unique, then we risk looping through values.)
    We use the last row in the previous page as the 'marker' for pagination.
    So we must return values that follow the passed marker in the order.
    With a single-valued sort_key, this would be easy: sort_key > X.
    With a compound-values sort_key, (k1, k2, k3) we must do this to repeat
    the lexicographical ordering:
    (k1 > X1) or (k1 == X1 && k2 > X2) or (k1 == X1 && k2 == X2 && k3 > X3)
    We also have to cope with different sort_directions.
    Typically, the id of the last row is used as the client-facing pagination
    marker, then the actual marker object must be fetched from the db and
    passed in to us as marker.
    :param query: the query object to which we should add paging/sorting
    :param model: the ORM model class
    :param limit: maximum number of items to return
    :param sort_keys: array of attributes by which results should be sorted
    :param marker: the last item of the previous page; we returns the next
                    results after this value.
    :param sort_dir: direction in which results should be sorted (asc, desc)
    :param sort_dirs: per-column array of sort_dirs, corresponding to sort_keys
    :rtype: sqlalchemy.orm.query.Query
    :return: The query with sorting/pagination added.
    """

    return query

    if 'id' not in sort_keys:
        # TODO(justinsb): If this ever gives a false-positive, check
        # the actual primary key, rather than assuming its id
        LOG.warn('Id not in sort_keys; is sort_keys unique?')

    assert(not (sort_dir and sort_dirs))

    # Default the sort direction to ascending
    if sort_dirs is None and sort_dir is None:
        sort_dir = 'asc'

    # Ensure a per-column sort direction
    if sort_dirs is None:
        sort_dirs = [sort_dir for _sort_key in sort_keys]

    assert(len(sort_dirs) == len(sort_keys))

    # Add sorting
    for current_sort_key, current_sort_dir in zip(sort_keys, sort_dirs):
        sort_dir_func = {
            'asc': sqlalchemy.asc,
            'desc': sqlalchemy.desc,
        }[current_sort_dir]

        try:
            sort_key_attr = getattr(model, current_sort_key)
        except AttributeError:
            raise Exception("exception.InvalidSortKey()")
        query = query.order_by(sort_dir_func(sort_key_attr))

    default = ''  # Default to an empty string if NULL

    # Add pagination
    if marker is not None and False:
        marker_values = []
        for sort_key in sort_keys:
            v = getattr(marker, sort_key)
            if v is None:
                v = default
            marker_values.append(v)

        # Build up an array of sort criteria as in the docstring
        criteria_list = []
        for i in range(len(sort_keys)):
            crit_attrs = []
            for j in range(i):
                model_attr = getattr(model, sort_keys[j])
                default = None if isinstance(
                    model_attr.property.columns[0].type,
                    sqlalchemy.DateTime) else ''
                attr = sa_sql.expression.case([(model_attr != None,
                                              model_attr), ],
                                              else_=default)
                crit_attrs.append((attr == marker_values[j]))

            model_attr = getattr(model, sort_keys[i])
            default = None if isinstance(model_attr.property.columns[0].type,
                                         sqlalchemy.DateTime) else ''
            attr = sa_sql.expression.case([(model_attr != None,
                                          model_attr), ],
                                          else_=default)
            if sort_dirs[i] == 'desc':
                crit_attrs.append((attr < marker_values[i]))
            elif sort_dirs[i] == 'asc':
                crit_attrs.append((attr > marker_values[i]))
            else:
                raise ValueError("Unknown sort direction, "
                                   "must be 'desc' or 'asc'")

            criteria = and_(*crit_attrs)
            criteria_list.append(criteria)

        f = or_(*criteria_list)
        query = query.filter(f)

    if limit is not None:
        query = query.limit(limit)

    return query

def image_get_all(context, filters=None, marker=None, limit=None,
                  sort_key=None, sort_dir=None,
                  member_status='accepted', is_public=None,
                  admin_as_user=False, return_tag=False):
    """
    Get all images that match zero or more filters.

    :param filters: dict of filter keys and values. If a 'properties'
                    key is present, it is treated as a dict of key/value
                    filters on the image properties attribute
    :param marker: image id after which to start page
    :param limit: maximum number of images to return
    :param sort_key: list of image attributes by which results should be sorted
    :param sort_dir: directions in which results should be sorted (asc, desc)
    :param member_status: only return shared images that have this membership
                          status
    :param is_public: If true, return only public images. If false, return
                      only private and shared images.
    :param admin_as_user: For backwards compatibility. If true, then return to
                      an admin the equivalent set of images which it would see
                      if it was a regular user
    :param return_tag: To indicates whether image entry in result includes it
                       relevant tag entries. This could improve upper-layer
                       query performance, to prevent using separated calls
    """
    sort_key = ['created_at'] if not sort_key else sort_key

    default_sort_dir = 'desc'

    if not sort_dir:
        sort_dir = [default_sort_dir] * len(sort_key)
    elif len(sort_dir) == 1:
        default_sort_dir = sort_dir[0]
        sort_dir *= len(sort_key)

    filters = filters or {}

    visibility = filters.pop('visibility', None)
    showing_deleted = 'changes-since' in filters or filters.get('deleted',
                                                                False)

    img_cond, prop_cond, tag_cond = _make_conditions_from_filters(
        filters, is_public)

    query = _select_images_query(context,
                                 img_cond,
                                 admin_as_user,
                                 member_status,
                                 visibility)

    if visibility is not None:
        if visibility == 'public':
            query = query.filter(models.Image.is_public == True)
        elif visibility == 'private':
            query = query.filter(models.Image.is_public == False)

    if prop_cond:
        for prop_condition in prop_cond:
            query = query.join(models.ImageProperty, aliased=True).filter(
                and_(*prop_condition))

    if tag_cond:
        for tag_condition in tag_cond:
            query = query.join(models.ImageTag, aliased=True).filter(
                and_(*tag_condition))

    marker_image = None
    if marker is not None:
        marker_image = _image_get(context,
                                  marker,
                                  force_show_deleted=showing_deleted)

    for key in ['created_at', 'id']:
        if key not in sort_key:
            sort_key.append(key)
            sort_dir.append(default_sort_dir)

    query = _paginate_query(query, models.Image, limit,
                            sort_key,
                            marker=marker_image,
                            sort_dir=None,
                            sort_dirs=sort_dir)

    query = query.options(sa_orm.joinedload(
        models.Image.properties)).options(
            sa_orm.joinedload(models.Image.locations))
    if return_tag:
        query = query.options(sa_orm.joinedload(models.Image.tags))

    images = []
    for image in query.all():
        if type(image) is list:
            print("""[DEBUG_GLANCE] image_get_all(context=%s, filters=%s, marker=%s, limit=%s,
                  sort_key=%s,                        sort_dir=%s,
                  member_status=%s,            is_public=%s,
                  admin_as_user=%s, return_tag=%s)""" % (context, filters, marker, limit,
                  sort_key,                        sort_dir,
                  member_status,            is_public,
                  admin_as_user, return_tag))
        image_dict = image.to_dict()
        image_dict = _normalize_locations(context, image_dict,
                                          force_show_deleted=showing_deleted)
        if return_tag:
            image_dict = _normalize_tags(image_dict)
        images.append(image_dict)
    return images

def _normalize_tags(image):
    undeleted_tags = filter(lambda x: not x.deleted, image['tags'])
    image['tags'] = [tag['value'] for tag in undeleted_tags]
    return image

class Context(object):
    def __init__(self, project_id, user_id, can_see_deleted, is_admin):
        self.project_id = project_id
        self.user_id = user_id
        self.can_see_deleted = can_see_deleted
        self.is_admin = is_admin


image_id = "7f777fd4-48d9-4491-be0b-b1e2bb1b2771"

def create_mock_data():
    image_id = "7f777fd4-48d9-4491-be0b-b1e2bb1b2771"
    values = {
        "container_format": "ami",
        "min_ram": 0,
        "_rid": "8b639fa8-cb5c-11e5-9557-080027d079db",
        "updated_at": {
            "timezone": "None",
            "simplify_strategy": "datetime",
            "value": "2016-02-04 16:30:09"
        },
        "owner": "47ab894478b542c3bcd03693ce3e979e",
        "_metadata_novabase_classname": "Image",
        "deleted_at": None,
        "id": image_id,
        "size": None,
        "disk_format": "ami",
        "_rome_version_number": 0,
        "status": "queued",
        "deleted": False,
        "_session": None,
        "min_disk": 0,
        "is_public": True,
        "virtual_size": None,
        "name": "cirros-0.3.4-x86_64-uec",
        "checksum": None,
        "_pid": "0xb3ac40ac",
        "_nova_classname": "images",
        "protected": False,
        "created_at": {
            "timezone": "None",
            "simplify_strategy": "datetime",
            "value": "2016-02-04 16:30:09"
        }
    }
    image = models.Image()
    image.update(values)
    image.save()

    values = {
        "name": "kernel_id",
        "_session": None,
        "deleted": False,
        "created_at": {
            "timezone": "None",
            "simplify_strategy": "datetime",
            "value": "2016-02-04 16:30:09"
        },
        "_rid": "8b639fa8-cb5c-11e5-9557-080027d079db",
        "updated_at": {
            "timezone": "None",
            "simplify_strategy": "datetime",
            "value": "2016-02-04 16:30:09"
        },
        "value": "c3707adb-6e7d-4e0b-ae5c-1ff9aff7ee2c",
        "id": 1,
        "image_id": image_id,
        "_rome_version_number": 0,
        "_nova_classname": "image_properties",
        "_metadata_novabase_classname": "ImageProperty",
        "deleted_at": None,
        "_pid": "0xb3ab9ccc"
    }
    image_property = models.ImageProperty()
    image_property.update(values)
    image_property.save()

    values = {
        "status": "active",
        "_rome_version_number": 0,
        "_session": None,
        "deleted": False,
        "_pid": "0xb3ae190c",
        "_rid": "8b639fa8-cb5c-11e5-9557-080027d079db",
        "updated_at": {
            "timezone": "None",
            "simplify_strategy": "datetime",
            "value": "2016-02-04 16:30:07"
        },
        "value": "file:///opt/stack/data/glance/images/c3707adb-6e7d-4e0b-ae5c-1ff9aff7ee2c",
        "id": 1,
        "image_id": image_id,
        "meta_data": {},
        "_nova_classname": "image_locations",
        "_metadata_novabase_classname": "ImageLocation",
        "deleted_at": None,
        "created_at": {
            "timezone": "None",
            "simplify_strategy": "datetime",
            "value": "2016-02-04 16:30:07"
        }
    }
    image_location = models.ImageLocation()
    image_location.update(values)
    image_location.save()

def test_marker():
    result = image_get_all(context, marker=image_id)
    print(result)

def test_marker():
    result = image_get_all(context, )
    print(result)

def test_image_get_all_devstack(context):
    from test.glance.api import image_get_all as image_get_all_api
    result = image_get_all_api(context, filters={'deleted': False}, marker=None, limit=25,
                  sort_key=['created_at', 'id'], sort_dir=['desc', 'desc'],
                  member_status="accepted", is_public=None,
                  admin_as_user=False, return_tag=True)
    print(result)

if __name__ == "__main__":

    context = Context("project1", "user1", True, True)

    if Query(models.Image).count() == 0:
        create_mock_data()

    # result = Query(models.Image.id, models.ImageMember.id, models.Image).join(models.ImageMember).all()
    # print(result)

    # test_marker()
    test_image_get_all_devstack(context)
