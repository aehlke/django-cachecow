import hashlib
from itertools import chain, imap
import logging
import re
import string
import time

import django
from django.core.cache import cache

from cachecow.intpacker import pack_int


logger = logging.getLogger(__name__)

# A memcached limit.
MAX_KEY_LENGTH = 250

# String containing all invalid characters to strip from memcached keys.
# Contains all control characters from C0 (0x00-0x20 and 0x7F),
# and C1 (0x80-0x9F) as defined by the ISO-2022 and ECMA-48 standards.
#
# References:
# http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
# http://www.unicode.org/charts/PDF/U0000.pdf
# http://www.unicode.org/charts/PDF/U0080.pdf
_CONTROL_CODE_CHARS = ''.join(chr(i) for i in chain(xrange(0x20 + 1),
                                                    [0x7f],
                                                    xrange(0x80, 0x9f + 1)))

# String containing all ASCII characters, used for string#translate which
# is an efficient way of deleting characters from a string.
_ALL_CHARS = string.maketrans('', '')


def key_arg_iterator(key_args, max_depth=1):
    '''
    Yields items from key arguments as we allow them in `cached_function` et al.

    This means `key_args` may be an iterable, or a single atomic object. If
    it's a string, we treat it as an atomic object rather than an iterable. For
    other iterables though, we traverse recursively up to `max_depth` levels
    deep, if they contain nested iterables.
    '''
    # Try traversing deeper into the input, unless it's a string.
    if max_depth >= 0 and not isinstance(key_args, basestring):
        try:
            for x in key_args:
                for y in key_arg_iterator(x, max_depth=max_depth - 1):
                    yield y
        except TypeError: # It's not an iterable, so forget recursing.
            yield key_args
    else:
        # Treat as atom.
        yield key_args


def _format_key_arg(arg):
    '''
    Selectively formats args passed to `make_key`. Defaults to serializing
    into a Unicode string and then encoding in UTF-8.
    '''
    to_string = lambda x: unicode(x).encode('utf8')

    if arg is None:
        return ''
    elif isinstance(arg, dict):
        # `str` is wasteful for dicts for our purposes here, so let's compact.
        s = ','.join([to_string(key) + ':' + to_string(val)
                      for key,val in arg.items()])
    else:
        s = to_string(arg)

    # Strip control characters and spaces (which memcached won't allow).
    return s.translate(_ALL_CHARS, _CONTROL_CODE_CHARS)


def make_key(obj, namespace=None):
    '''
    Returns a string serialization of `obj` which is usable as a cache key.

    This function is used internally by CacheCow, but it's exposed in case you 
    want to use it directly with the lower-level Django cache API without the
    rest of CacheCow, and so that you can see how keys are constructed.

    This does a couple things to turn the given object into a clean key:

        1. Recursively traverses, serializes and joins together any iterables, 
           so you can pass a list of items to be turned into a key. The 
           recursion depth is limited to 1 level.
       
           String objects are an exception to this -- they are treated here as 
           atomic units, despite being iterables.

        2. Removes any control code characters and spaces [1] (which are
           illegal in memcached keys [2].)

        3. After the above two steps, if the resulting length is >
           MAX_KEY_LENGTH bytes (250 by default, which is the memcached 
           protocol limit), it generates a hash out of the key instead.
    
    It's possible the resulting key would serialize into an empty string, so
    choose your args carefully to avoid this.

    [1] http://www.unicode.org/charts/PDF/U0000.pdf
        http://www.unicode.org/charts/PDF/U0080.pdf

    [2] http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
    '''
    key = '.'.join(imap(_format_key_arg, key_arg_iterator(obj)))

    if namespace is not None:
        namespace = make_key(namespace)
        key = '{}:{}'.format(_get_namespace_prefix(namespace), key)

    # Use cache.key_prefix if available (Django>=1.3), otherwise CACHE_KEY_PREFIX.
    if (not hasattr(cache, 'key_prefix')
            and getattr(settings, 'CACHE_KEY_PREFIX', None)):
        key = '{}:{}'.format(settings.KEY_PREFIX, key)

    try:
        django_key = cache.make_key(key)
    except AttributeError:
        django_key = key

    # If the resulting key is too long, hash the part after the prefix, and
    # truncate as needed.
    if len(key) > MAX_KEY_LENGTH:
        try:
            # Django 1.3+ prepends some stuff to keys.
            prefix = cache.make_key('')
        except AttributeError:
            prefix = ''
        
        # Just to be safe... we should be able to have a key >= 1 char long :)
        if len(prefix) >= MAX_KEY_LENGTH:
            raise Exception('Your cache key prefixes are too long.')

        #TODO a further refinement of this would be to hash only the smallest 
        # part necessary to get it under the limit. Don't hash an entire key 
        # just for being 1 char too long. This would improve readability.
        key = hashlib.md5(key).hexdigest()[:MAX_KEY_LENGTH - len(prefix)]
    return key


def _make_namespace_prefix():
    '''
    Returns a likely-to-be-unique value that can be incremented with
    `cache.incr`, so that namespace invalidation can be atomic.
    '''
    # It only needs to be unique within the given namespace, and the only risk
    # of collision would be from a *deleted* namespace key, which should only
    # happen because memcached etc. ejected it for being too old. So there's
    # little risk to using the current time as the value.
    #
    # Use (an overly-cautious) time-since-epoch modulo decade, in nanoseconds.
    # It doesn't save many characters to be less cautious, so it's fine.
    decade_s = 3600 * 24 * 365 * 10 # decade in seconds.
    return int((time.time() % decade_s) * 1e9)


def _get_namespace_prefix(namespace):
    '''
    Gets (or sets if uninitialized) the key prefix for the given namespace 
    string. The return value will prepend any keys that belong to the namespace.
    '''
    namespace = make_key(namespace)
    ns_key = cache.get(namespace)
    if not ns_key:
        ns_key = _make_namespace_prefix()
        cache.set(namespace, ns_key)

    # Compact the key before returning it to save space when using it.
    return pack_int(ns_key)


def invalidate_namespace(namespace):
    '''
    If the namespace is already invalid (i.e. the namespace key has been 
    deleted from the cache), this does nothing.

    This operation is atomic as long as the cache backend's `incr` is too.

    It is an O(1) operation, independent of the number of keys in a namespace.
    '''
    namespace = make_key(namespace)

    logger.debug('invalidating namespace: {0}'.format(namespace))
    #logger.debug('namespace value was: {0}'.format(cache.get(namespace)))

    try:
        cache.incr(namespace)
    except ValueError:
        # The namespace is already invalid, since its key is gone.
        pass


def timedelta_to_seconds(t):
    '''
    Returns an int.

    Tries to use Python 2.7's timedelta#total_seconds, if available.
    '''
    try:
        return int(t.total_seconds())
    except AttributeError:
        return int(t.microseconds + (t.seconds + t.days * 3600 * 24))


def set_cache(key, val, timeout=None, namespace=None, **kwargs):
    '''
    Wrapper around cache.set to allow either int or timedelta timeouts,
    and optional namespace support.

    Passes `kwargs` on to `cache.set` for Django 1.3+'s optional `version`
    parameter.
    '''
    try:
        timeout = timedelta_to_seconds(timeout)
    except AttributeError:
        pass

    if timeout and timeout < 0:
        raise Exception('Cache timeout value must not be negative.')

    logger.debug(u'setting cache: {} = {} ({}, timeout={})'.format(
        key, val, val.__class__, timeout))

    cache.set(key, val, timeout=timeout, **kwargs)

