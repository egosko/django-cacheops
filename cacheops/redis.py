from __future__ import absolute_import
import warnings
import six
from logging import getLogger
from funcy import decorator, identity, memoize
import redis
from django.core.exceptions import ImproperlyConfigured

from .conf import settings

logger = getLogger(__name__)


if settings.CACHEOPS_DEGRADE_ON_FAILURE:
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


Redis = SafeRedis if settings.CACHEOPS_DEGRADE_ON_FAILURE else redis.StrictRedis


class LazyRedis(object):
    def _setup(self):
        if not settings.CACHEOPS_REDIS:
            raise ImproperlyConfigured('You must specify CACHEOPS_REDIS setting to use cacheops')

        # Allow client connection settings to be specified by a URL.
        if isinstance(settings.CACHEOPS_REDIS, six.string_types):
            client = Redis.from_url(settings.CACHEOPS_REDIS)
        else:
            client = Redis(**settings.CACHEOPS_REDIS)

        object.__setattr__(self, '__class__', client.__class__)
        object.__setattr__(self, '__dict__', client.__dict__)

    def __getattr__(self, name):
        self._setup()
        return getattr(self, name)

    def __setattr__(self, name, value):
        self._setup()
        return setattr(self, name, value)

try:
    redis_conf = settings.CACHEOPS_REDIS
    redis_replica_conf = settings.CACHEOPS_REDIS_REPLICA
    redis_replica = redis.StrictRedis(**redis_replica_conf)

    class ReplicaProxyRedis(Redis):
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


### Lua script loader

import re
import os.path

STRIP_RE = re.compile(r'TOSTRIP.*/TOSTRIP', re.S)

@memoize
def load_script(name, strip=False):
    filename = os.path.join(os.path.dirname(__file__), 'lua/%s.lua' % name)
    with open(filename) as f:
        code = f.read()
    if strip:
        code = STRIP_RE.sub('', code)
    return redis_client.register_script(code)