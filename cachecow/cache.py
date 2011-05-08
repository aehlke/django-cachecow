from datetime import timedelta
from django.conf import settings
from django.contrib import messages
from django.core.cache import cache
from django.http import HttpRequest
from django.utils import translation
from functools import wraps
from intpacker import pack_int
from itertools import chain
import hashlib
import inspect
import re
import string
import time


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

def _format_key_arg(arg):
    '''
    Selectively formats args passed to `make_key`. Defaults to serializing
    into a Unicode string and then encoding in UTF-8.
    '''
    to_string = lambda x: unicode(x).encode('utf8')

    if isinstance(arg, dict):
        # `str` is wasteful for dicts, for our case here.
        s = ','.join([to_string(key) + ':' + to_string(val)
                      for key,val in arg.items()])
    else:
        s = to_string(arg)

    # Strip control characters and spaces (which memcached won't allow).
    return s.translate(_ALL_CHARS, _CONTROL_CODE_CHARS)

def make_key(*args):
    '''
    This does a couple things to cleanly make a key out of the given arguments:

        1. Removes any control code characters and spaces [1] (which are
           illegal in memcached keys [2].)

        2. After serializing all arguments and joining them into one string, if
           the resulting length is > MAX_KEY_LENGTH bytes (250 by default, 
           which is the memcached protocol limit), it generates a hash out of 
           the key instead.
    
    It's possible the resulting key would be empty, so choose your args 
    carefully to avoid this.

    TODO a further refinement of this would be to hash only the smallest part
    necessary to get it under the limit. Don't hash an entire key just for 
    being 1 char too long. This would improve readability.

    [1] http://www.unicode.org/charts/PDF/U0000.pdf
        http://www.unicode.org/charts/PDF/U0080.pdf

    [2] http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
    '''
    key = '.'.join(map(_format_key_arg, filter(lambda x: x is not None, args)))

    # If our key is too long, hash the part after the prefix,
    # and truncate as needed.
    if len(cache.make_key(key)) > MAX_KEY_LENGTH:
        prefix = cache.make_key('')
        
        # Just to be safe... we should be able to have a key >= 1 char long :)
        if len(prefix) >= MAX_KEY_LENGTH:
            raise Exception('Your cache key prefixes are too long.')

        key = hashlib.md5(key).hexdigest()[:MAX_KEY_LENGTH - len(prefix)]
    return key

def _make_keys_from_function(func, *args, **kwargs):
    '''
    Add a bunch of hopefully uniquely identifying parameters to a list to be 
    passed to `make_key`. It's pretty crafty in finding distinguishing params
    to use, but it is slightly conservative so as to not overrun the memcached 
    key length limit, which results in a non-human-readable hash for a key.
    '''
    keys = ['cached_func', func.__name__]

    # This works on both functions and class methods.
    signature_args = inspect.getargspec(func).args
    if (inspect.ismethod(func)
        or (signature_args and signature_args[0] in ['self', 'cls'])):
        # Method, probably. 
        #
        # If ismethod returns True, it's definitely a method. Otherwise,
        # we have to guess based on the first arg of the function's signature.
        # This is the best guess we can have in Python, because the way a 
        # function is passed to a decorator inside a class definition, the
        # decorated function is as yet neither a bound nor unbound method. It's
        # just a regular function. So we must guess from its args.
        #
        # A guess is good enough, since it only means that we add some extra
        # fields from the first arg. If we're wrong, the key is more
        # conservative (more detailed) than need be. We could wrongly call it a
        # function when it's actually a method, but only if they're doing 
        # unsightly things like naming the "self" or "cls" arg something else.
        self = args[0]
        keys.append(self.__class__.__name__)
        if hasattr(self, 'pk'): # django model? `pk` is a great differentiator!
            keys.append(self.pk)
        keys.extend(args[1:])
    else:
        # Function.
        keys.extend(args)
    keys.extend(kwargs.values())

    # To be extra safe! (unreadable, so at end of key.)
    # If this results in any collisions, it actually won't make a difference.
    # It's fine to memoize functions that collide on this as if
    # they are one, since they're identical if their codeblock hash is the same.
    keys.append(pack_int(func.__code__.__hash__()))

    return keys

def _timedelta_to_seconds(t):
    '''
    Returns an int.

    Tries to use Python 2.7's timedelta#total_seconds, if available.
    '''
    try:
        return int(t.total_seconds())
    except AttributeError:
        return int(t.microseconds + (t.seconds + t.days * 86400))

def _make_namespace_key(namespace):
    '''
    Returns a likely-to-be-unique value that can be incremented with 
    `cache.incr`.
    '''
    # Use (an overly-cautious) time-since-epoch modulo decade, in nanoseconds.
    decade_s = 3600 * 24 * 365 * 10 # decade in seconds.
    return int((time.time() % decade_s) * 1e9)

def _get_namespace_key(namespace):
    '''
    Gets (or sets if uninitialized) the key prefix for the given namespace. The
    return value is used to prefix any keys that belong to the namespace.
    '''
    ns_key = cache.get(namespace)
    if not ns_key:
        ns_key = _make_namespace_key(namespace)
        cache.set(namespace, ns_key)

    # Compact the key before returning it to save space when using it.
    return pack_int(ns_key)

def _process_namespace_name(namespace):
    '''
    A namespace can be any serializable object or list of objects, not just a
    string. This serializes the namespace name by passing it to `make_key`.
    '''
    # Don't explode strings.
    if isinstance(namespace, str):
        return make_key(namespace)

    # First try as if it's an iterable.
    try:
        return make_key(*namespace)
    except TypeError: # It might not be an iterable.
        return make_key(namespace)

def invalidate_namespace(namespace):
    '''
    If the namespace is already invalid (i.e. the namespace key has been 
    deleted from the cache), this does nothing.

    This operation is atomic as long as the cache backend's `incr` is too.

    It is an O(1) operation, independent of the number of keys in a namespace.
    '''
    namespace = _process_namespace_name(namespace)
    try:
        cache.incr(namespace)
    except ValueError:
        # The namespace is already invalid, since its key is gone.
        pass

def _make_key(keys, namespace, func, args, kwargs):
    '''
    Returns the cache key to use for the decorated function. Calls and replaces 
    any callable items in `keys` with their return values before sending `keys`
    over to `make_key`. Does the same for a callable `namespace`.
    '''
    keys = keys or _make_keys_from_function(func, *args, **kwargs)

    def call_if_callable(key_arg):
        if callable(key_arg):
            return key_arg(*args, **kwargs)
        return key_arg

    keys = map(call_if_callable, keys)

    namespace = call_if_callable(namespace)
    if namespace:
        namespace = _process_namespace_name(namespace)
        keys.append(_get_namespace_key(namespace))

    return make_key(*keys)

def _set_cache(key, val, timeout):
    '''
    Wrapper around cache.set to allow timedelta timeouts.
    '''
    if isinstance(timeout, timedelta):
        timeout = _timedelta_to_seconds(timeout)
    if timeout and timeout < 0:
        raise Exception('Cache timeout value must not be negative.')
    cache.set(key, val, timeout=timeout)

def _add_delete_cache_member(func, keys=None, namespace=None):
    '''
    Adds an `delete_cache` member function to `func`. Pass it the same args
    as `func` so that it can find the right key to delete.

    If `func` was decorated with `cached_function` or `cached_view` with a
    `keys` parameter specified, `delete_cache` takes no arguments.
    '''
    def delete_cache(*args, **kwargs):
        key = _make_key(keys, namespace, func, args, kwargs)
        cache.delete(key)
    func.delete_cache = delete_cache

def cached_function(timeout=None, keys=None, namespace=None):
    '''
    Adds a kwarg to the function, `invalidate_cache`. This allows the function
    to setup signal listeners for invalidating the cache.

    Works on both functions and class methods.

    All kwargs, `timeout`, `keys` and `namespace`, are optional.

    `timeout` can be either an int, or a timedelta (or None).

    `keys` is used to create a key for the cached value. It must be an iterable.
    The items in it are iterated over, serialized into strings, formatted to 
    remove any illegal characters, then joined into one string to use as the
    key. If `keys` is None, we'll automatically create a determinatively and
    uniquely identifying key for the function which is hopefully human-readable.

    Any key in `keys` can be callable, as well. These will be called with the 
    same args and kwargs as the decorated function, and their return values 
    will be serialized and added to the key.

    `namespace` is used as an alternative way to invalidate a key or a group of
    keys. When `namespace` is used, all keys that belong to the given namespace
    can be invalidated simply by passing the namespace name to
    `invalidate_namespace`. This is especially helpful when you start worker 
    processes that don't know what's already in the cache, since it relieves 
    you of needing to keep track of every key you set.

    `namespace` can be either a string (or anything serializable) or a
    function. If a function, it will be called with the same arguments as the
    function that `cached_function` is decorating. The return value will be
    serialized using `make_key`, so you can return a string or an iterable of
    things to be serialized. This lets you create namespaces dynamically 
    depending on some of the arguments passed to whatever you're caching.
    For example, you may want to cache several functions in the same namespace
    depending on the current user.
    
    Note that the `namespace` function *must* be determinstic -- given the same
    input arguments, it must always produce the same output.
    '''
    def decorator(func):
        _add_delete_cache_member(func, keys=keys, namespace=namespace)

        @wraps(func)
        def wrapped(*args, **kwargs):
            key = _make_key(keys, namespace, func, args, kwargs)
            val = cache.get(key)

            if val is None:
                val = func(*args, **kwargs)
                _set_cache(key, val, timeout)
            return val
        return wrapped
    return decorator

def _can_cache_request(request):
    '''
    Only caches if the request is for GET or HEAD, and if the Django messages
    app has no messages available for the user.
    '''
    if len(messages.get_messages(request)) != 0:
        return False
    return request.method in ['GET', 'HEAD']

def _can_cache_response(response):
    # Only set the cache if the HTTP response code is 200.
    return (response.status_code != 200
            or 'no-cache' in response.get('Cache-Control', '')
            or 'no-cache' in response.get('Pragma', ''))

def cached_view(timeout=None, keys=None, namespace=None, add_user_to_key=False):
    '''
    Use this instead of `cached_function` for caching views.  See 
    `cached_function` for documentation on how to use this.

    Handles HttpRequest objects intelligently when auto-generating the 
    cache key.

    Only caches GET and HEAD requests which have an HTTP 200 response code.
    Doesn't cache responses which have "Cache-Control: no-cache" or 
    "Pragma: no-cache" in the headers.

    If `add_user_to_key` is True, the key will be prefixed with the user's ID,
    if logged in.
    '''
    def decorator(func):
        _add_delete_cache_member(func, keys=keys, namespace=namespace)

        @wraps(func)
        def wrapped(request, *args, **kwargs):
            if not _can_cache_request(request):
                return func(request, *args, **kwargs)
            
            _keys = keys

            # Default keys.
            if not _keys:
                # Don't naively add the `request` arg to the cache key.
                _keys = _make_keys_from_function(func, *args, **kwargs)
                
                # Only add specific parts of the `request` object to the key.
                _keys.extend(chain.from_iterable(request.GET.items()))
                _keys.append(request.method)

                # Add the current language.
                _keys.append(translation.get_language())

                # Current site, if available.
                _keys.append(getattr(settings, 'SITE_ID', None))

            try:
                if add_user_to_key and request.user.is_authenticated():
                    _keys.append(request.user.id)
            except AttributeError: # maybe "auth" isn't installed.
                pass
            
            key = _make_key(_keys, namespace, func, args, kwargs)

            val = cache.get(key)
            if val is None:
                val = func(request, *args, **kwargs)
                
                if _can_cache_response(val):
                    _set_cache(key, val, timeout)
            return val
        return wrapped
    return decorator



