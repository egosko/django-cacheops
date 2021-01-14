# -*- coding: utf-8 -*-
from copy import deepcopy
from logging import getLogger
import warnings
import six
from funcy import memoize, merge

from django.conf import settings as base_settings
from django.core.exceptions import ImproperlyConfigured

logger = getLogger(__name__)

ALL_OPS = {'get', 'fetch', 'count', 'exists'}


class Settings(object):
    CACHEOPS_ENABLED = True
    CACHEOPS_REDIS = {}
    CACHEOPS_DEFAULTS = {}
    CACHEOPS = {}
    CACHEOPS_LRU = False
    CACHEOPS_DEGRADE_ON_FAILURE = False
    FILE_CACHE_DIR = '/tmp/cacheops_file_cache'
    FILE_CACHE_TIMEOUT = 60*60*24*30

    def __getattribute__(self, name):
        if hasattr(base_settings, name):
            return getattr(base_settings, name)
        return object.__getattribute__(self, name)


settings = Settings()

LRU = getattr(settings, 'CACHEOPS_LRU', False)
DEGRADE_ON_FAILURE = getattr(settings, 'CACHEOPS_DEGRADE_ON_FAILURE', False)


# Support DEGRADE_ON_FAILURE
if DEGRADE_ON_FAILURE:
    @decorator
    def handle_connection_failure(call):
        try:
            return call()
        except redis.ConnectionError as e:
            warnings.warn("The cacheops cache is unreachable! Error: %s" % e, RuntimeWarning)
        except redis.TimeoutError as e:
            warnings.warn("The cacheops cache timed out! Error: %s" % e, RuntimeWarning)
        except redis.RedisError as e:
            logger.exception(e)
else:
    handle_connection_failure = identity


class SafeRedis(redis.StrictRedis):
    get = handle_connection_failure(redis.StrictRedis.get)


try:
    redis_conf = settings.CACHEOPS_REDIS
except AttributeError:
    raise ImproperlyConfigured('You must specify non-empty CACHEOPS_REDIS setting to use cacheops')

CacheopsRedis = SafeRedis if DEGRADE_ON_FAILURE else redis.StrictRedis

class LazyRedis(object):
    def _setup(self):
        # Connecting to redis
        client = CacheopsRedis(**redis_conf)

        object.__setattr__(self, '__class__', client.__class__)
        object.__setattr__(self, '__dict__', client.__dict__)

    def __getattr__(self, name):
        self._setup()
        return getattr(self, name)

    def __setattr__(self, name, value):
        self._setup()
        return setattr(self, name, value)


try:
    redis_replica_conf = settings.CACHEOPS_REDIS_REPLICA
    redis_replica = redis.StrictRedis(**redis_replica_conf)

    class ReplicaProxyRedis(CacheopsRedis):
        """ Proxy `get` calls to redis replica.
        """
        def get(self, *args, **kwargs):
            try:
                return redis_replica.get(*args, **kwargs)
            except redis.TimeoutError:
                logger.exception("TimeoutError occured while reading from replica")
            except redis.ConnectionError:
                pass
            except redis.RedisError as e:
                logger.exception(e)
            return super(ReplicaProxyRedis, self).get(*args, **kwargs)

    redis_client = ReplicaProxyRedis(**redis_conf)
except AttributeError:
    redis_client = LazyRedis()


@memoize
def prepare_profiles():
    """
    Prepares a dict 'app.model' -> profile, for use in model_profile()
    """
    profile_defaults = {
        'ops': (),
        'local_get': False,
        'db_agnostic': True,
        'write_only': False,
        'lock': False,
    }
    profile_defaults.update(settings.CACHEOPS_DEFAULTS)

    model_profiles = {}
    for app_model, profile in settings.CACHEOPS.items():
        if profile is None:
            model_profiles[app_model.lower()] = None
            continue

        model_profiles[app_model.lower()] = mp = merge(profile_defaults, profile)
        if mp['ops'] == 'all':
            mp['ops'] = ALL_OPS
        # People will do that anyway :)
        if isinstance(mp['ops'], six.string_types):
            mp['ops'] = {mp['ops']}
        mp['ops'] = set(mp['ops'])

        if 'timeout' not in mp:
            raise ImproperlyConfigured(
                'You must specify "timeout" option in "%s" CACHEOPS profile' % app_model)

    return model_profiles


def model_profile(model):
    """
    Returns cacheops profeile for a model
    """
    if model_is_fake(model):
        return None

    model_profiles = prepare_profiles()

    app = model._meta.app_label.lower()
    model_name = model._meta.model_name
    for guess in ('%s.%s' % (app, model_name), '%s.*' % app, '*.*'):
        if guess in model_profiles:
            return model_profiles[guess]
    else:
        return None


def model_is_fake(model):
    return model.__module__ == '__fake__'
