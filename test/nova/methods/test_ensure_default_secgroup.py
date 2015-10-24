__author__ = 'jonathan'

import test.nova._fixtures as models
from lib.rome.core.orm.query import Query

from lib.rome.core.orm.query import Query as RomeQuery
from lib.rome.core.session.session import Session as RomeSession

import logging

def get_session(use_slave=False, **kwargs):
    # return FakeSession()
    return RomeSession()
    # return OldRomeSession()


def model_query(context, *args, **kwargs):
    # base_model = kwargs["base_model"]
    # models = args
    return RomeQuery(*args, **kwargs)

def _quota_usage_create(project_id, user_id, resource, in_use,
                        reserved, until_refresh, session=None):
    quota_usage_ref = models.QuotaUsage()
    quota_usage_ref.project_id = project_id
    quota_usage_ref.user_id = user_id
    quota_usage_ref.resource = resource
    quota_usage_ref.in_use = in_use
    quota_usage_ref.reserved = reserved
    quota_usage_ref.until_refresh = until_refresh
    # updated_at is needed for judgement of max_age
    # quota_usage_ref.updated_at = timeutils.utcnow()

    quota_usage_ref.save(session=session)

    return quota_usage_ref

def _security_group_rule_create(context, values, session=None):
    security_group_rule_ref = models.SecurityGroupIngressRule()
    security_group_rule_ref.update(values)
    security_group_rule_ref.save(session=session)
    return security_group_rule_ref

def _security_group_create(context, values, session=None):
    security_group_ref = models.SecurityGroup()
    # FIXME(devcamcar): Unless I do this, rules fails with lazy load exception
    # once save() is called.  This will get cleaned up in next orm pass.
    security_group_ref.rules
    security_group_ref.update(values)
    try:
        security_group_ref.save(session=session)
    except Exception as e:
        raise e
    return security_group_ref

def _security_group_get_query(context, session=None, read_deleted=None,
                              project_only=False, join_rules=True):
    query = model_query(context, models.SecurityGroup, session=session,
            read_deleted=read_deleted, project_only=project_only)
    # if join_rules:
    #     query = query.options(joinedload_all('rules.grantee_group'))
    return query

def _security_group_get_by_names(context, session, project_id, group_names):
    """Get security group models for a project by a list of names.
    Raise SecurityGroupNotFoundForProject for a name not found.
    """
    query = _security_group_get_query(context, session=session,
                                      read_deleted="no", join_rules=False).\
            filter_by(project_id=project_id).\
            filter(models.SecurityGroup.name.in_(group_names))
    sg_models = query.all()
    if len(sg_models) == len(group_names):
        return sg_models
    # Find the first one missing and raise
    group_names_from_models = [x.name for x in sg_models]
    for group_name in group_names:
        if group_name not in group_names_from_models:
            raise Exception()
    # Not Reached

def _security_group_rule_get_default_query(context, session=None):
    return model_query(context, models.SecurityGroupIngressDefaultRule,
                       session=session)

def _security_group_ensure_default(context, session=None):
    if session is None:
        session = get_session()

    with session.begin(subtransactions=True):
        try:
            default_group = _security_group_get_by_names(context,
                                                         session,
                                                         context.project_id,
                                                         ['default'])[0]
        except Exception as e:
            values = {'name': 'default',
                      'description': 'default',
                      'user_id': context.user_id,
                      'project_id': context.project_id}
            default_group = _security_group_create(context, values,
                                                   session=session)
            usage = model_query(context, models.QuotaUsage,
                                read_deleted="no", session=session).\
                     filter_by(project_id=context.project_id).\
                     filter_by(user_id=context.user_id).\
                     filter_by(resource='security_groups')
            # Create quota usage for auto created default security group
            if not usage.first():
                _quota_usage_create(context.project_id,
                                    context.user_id,
                                    'security_groups',
                                    1, 0,
                                    None,
                                    session=session)
            else:
                usage_ref = usage.first()
                usage_ref.update({'in_use': int(usage_ref.in_use) + 1})
                # TODO (Jonathan): add a "session.add" to ease the session management :)
                session.add(usage_ref)

            default_rules = _security_group_rule_get_default_query(context,
                                session=session).all()
            for default_rule in default_rules:
                # This is suboptimal, it should be programmatic to know
                # the values of the default_rule
                rule_values = {'protocol': default_rule.protocol,
                               'from_port': default_rule.from_port,
                               'to_port': default_rule.to_port,
                               'cidr': default_rule.cidr,
                               'parent_group_id': default_group.id,
                }
                _security_group_rule_create(context,
                                            rule_values,
                                            session=session)
        return default_group

class Context(object):
    def __init__(self, project_id, user_id):
        self.project_id = project_id
        self.user_id = user_id

if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)

    context = Context("project1", "user1")
    _security_group_ensure_default(context)
