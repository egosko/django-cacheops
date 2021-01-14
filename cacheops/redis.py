from __future__ import absolute_import
import warnings
from contextlib import contextmanager
import six
from logging import getLogger

from django.core.signals import request_started, request_finished
from django.dispatch import receiver
from funcy import decorator, identity, memoize
import redis

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

    if settings.CACHEOPS_DEGRADE_TILL_REQUEST_FINISHED:
        degraded_client_set = set()


        class DegradedClientError(redis.RedisError):
            """ Error which raised after attempt to make a query to the degraded
            client.
            """


        @decorator
        def degrade_client_decorator(call):
            """ Decorator which marks client as degraded if Connection or Timeout
            error occured during the query.
            Query to the degraded client won't be executed at all.
            Degraded mark saves during the request.
            """
            if call.self in degraded_client_set:
                raise DegradedClientError()

            try:
                return call()
            except (redis.ConnectionError, redis.TimeoutError):
                degraded_client_set.add(call.self)
                raise


        @receiver([request_started, request_finished])
        def clear_degraded_client_marks(*args, **kwargs):
            """ Clear all degraded client marks which was setted in current or
            previous request.
            """
            degraded_client_set.clear()
    else:
        degrade_client_decorator = identity
else:
    handle_connection_failure = identity
    degrade_client_decorator = identity


LOCK_TIMEOUT = 60


class CacheopsRedis(redis.StrictRedis):
    get = handle_connection_failure(redis.StrictRedis.get)

    @contextmanager
    def getting(self, key, lock=False):
        if not lock:
            yield self.get(key)
        else:
            locked = False
            try:
                data = self._get_or_lock(key)
                locked = data is None
                yield data
            finally:
                if locked:
                    self._release_lock(key)

    @handle_connection_failure
    def _get_or_lock(self, key):
        self._lock = getattr(self, '_lock', self.register_script("""
            local locked = redis.call('set', KEYS[1], 'LOCK', 'nx', 'ex', ARGV[1])
            if locked then
                redis.call('del', KEYS[2])
            end
            return locked
        """))
        signal_key = key + ':signal'

        while True:
            data = self.get(key)
            if data is None:
                if self._lock(keys=[key, signal_key], args=[LOCK_TIMEOUT]):
                    return None
            elif data != b'LOCK':
                return data

            # No data and not locked, wait
            self.brpoplpush(signal_key, signal_key, timeout=LOCK_TIMEOUT)

    @handle_connection_failure
    def _release_lock(self, key):
        self._unlock = getattr(self, '_unlock', self.register_script("""
            if redis.call('get', KEYS[1]) == 'LOCK' then
                redis.call('del', KEYS[1])
            end
            redis.call('lpush', KEYS[2], 1)
            redis.call('expire', KEYS[2], 1)
        """))
        signal_key = key + ':signal'
        self._unlock(keys=[key, signal_key])


class SafeRedis(CacheopsRedis):
    get = handle_connection_failure(degrade_client_decorator(CacheopsRedis.get))
    evalsha = degrade_client_decorator(CacheopsRedis.evalsha)


Redis = SafeRedis if settings.CACHEOPS_DEGRADE_ON_FAILURE else CacheopsRedis


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

    redis_replica = CacheopsRedis(**redis_replica_conf)

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