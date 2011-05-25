from datetime import timedelta
from django.conf import settings
from django.contrib import messages
from django.core.cache import cache
from django.http import HttpRequest
from django.utils import translation
from functools import wraps
from intpacker import pack_int
from itertools import chain, imap
import hashlib
import inspect
import re
import string
import time
import logging

# Get an instance of a logger
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

def _key_arg_iterator(key_args, max_depth=1):
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
                for y in _key_arg_iterator(x, max_depth=max_depth - 1):
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

def make_key(obj):
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
    key = '.'.join(imap(_format_key_arg, _key_arg_iterator(obj)))

    # If the resulting key is too long, hash the part after the prefix, and
    # truncate as needed.
    if len(cache.make_key(key)) > MAX_KEY_LENGTH:
        prefix = cache.make_key('') # Django prepends some stuff to keys.
        
        # Just to be safe... we should be able to have a key >= 1 char long :)
        if len(prefix) >= MAX_KEY_LENGTH:
            raise Exception('Your cache key prefixes are too long.')

        #TODO a further refinement of this would be to hash only the smallest 
        # part necessary to get it under the limit. Don't hash an entire key 
        # just for being 1 char too long. This would improve readability.
        key = hashlib.md5(key).hexdigest()[:MAX_KEY_LENGTH - len(prefix)]
    return key

def _make_key_args_from_function(func, *args, **kwargs):
    '''
    Add a bunch of hopefully uniquely identifying parameters to a list to be 
    passed to `make_key`. It's pretty crafty in finding distinguishing params
    to use, but it is slightly conservative so as to not overrun the memcached 
    key length limit, which results in a non-human-readable hash for a key.
    '''
    key_args = ['cached_func', func.__name__]

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
        key_args.append(self.__class__.__name__)
        if hasattr(self, 'pk'): # django model? `pk` is a great differentiator!
            key_args.append(self.pk)
        key_args.extend(args[1:])
    else:
        # Function.
        key_args.extend(args)
    key_args.extend(kwargs.values())

    # To be extra safe! (unreadable, so at end of key.)
    # If this results in any collisions, it actually won't make a difference.
    # It's fine to memoize functions that collide on this as if
    # they are one, since they're identical if their codeblock hash is the same.
    key_args.append(pack_int(func.__code__.__hash__()))
    return key_args

def _make_namespace_prefix():
    '''
    Returns a likely-to-be-unique value that can be incremented with 
    `cache.incr`, so that namespace invalidation can be atomic. It only needs
    to be unique within the given namespace, and the only risk of collision 
    would be from a *deleted* namespace key, which should only happen because 
    memcached etc. ejected it for being too old. So there's little risk to 
    using the current time as the value.
    '''
    # Use (an overly-cautious) time-since-epoch modulo decade, in nanoseconds.
    # It doesn't save many characters to be less cautious, so it's fine.
    decade_s = 3600 * 24 * 365 * 10 # decade in seconds.
    return int((time.time() % decade_s) * 1e9)

def _get_namespace_prefix(namespace):
    '''
    Gets (or sets if uninitialized) the key prefix for the given namespace 
    string. The return value will prepend any keys that belong to the namespace.
    '''
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
    logger.debug('namespace value was: {0}'.format(cache.get(namespace)))

    try:
        cache.incr(namespace)
    except ValueError:
        # The namespace is already invalid, since its key is gone.
        pass

def _make_key(key_args, namespace, func_args, func_kwargs):
    '''
    Returns the cache key to use for the decorated function. Calls and replaces 
    any callable items in `key_args` with their return values before sending 
    `key_args` over to `make_key`. Does the same for a callable `namespace`.
    '''
    def call_if_callable(obj):
        if callable(obj):
            return obj(*func_args, **func_kwargs)
        return obj

    key_args = map(call_if_callable, _key_arg_iterator(key_args))

    namespace = call_if_callable(namespace)
    if namespace:
        key_args.append(_get_namespace_prefix(make_key(namespace)))

    logger.debug(u'_make_key passed namespace: {0}'.format(namespace))
    logger.debug(u'_make_key returning: {0}'.format(make_key(key_args)))
    return make_key(key_args)

def _timedelta_to_seconds(t):
    '''
    Returns an int.

    Tries to use Python 2.7's timedelta#total_seconds, if available.
    '''
    try:
        return int(t.total_seconds())
    except AttributeError:
        return int(t.microseconds + (t.seconds + t.days * 3600 * 24))

def _set_cache(key, val, timeout):
    '''
    Wrapper around cache.set to allow timedelta timeouts for our decorators.
    '''
    if isinstance(timeout, timedelta):
        timeout = _timedelta_to_seconds(timeout)
    if timeout and timeout < 0:
        raise Exception('Cache timeout value must not be negative.')

    logger.debug(u'setting cache: {0} = {1}'.format(key,val))
    cache.set(key, val, timeout=timeout)

def _add_delete_cache_member(func, key=None, namespace=None):
    '''
    Adds an `delete_cache` member function to `func`. Pass it the same args
    as `func` so that it can find the right key to delete.

    If `func` was decorated with `cached_function` or `cached_view` with a
    `key` parameter specified, `delete_cache` takes no arguments.
    '''
    def delete_cache(*args, **kwargs):
        key_args = key or _make_key_args_from_function(func, *args, **kwargs)
        _key = _make_key(key_args, namespace, args, kwargs)
        cache.delete(_key)
    func.delete_cache = delete_cache

def cached_function(timeout=None, key=None, namespace=None):
    '''
    Memoizes a function or class method using the Django cache backend. 

    Adds a member to the decorated function, `delete_cache`. Call it with the 
    same args as the decorated function.

    All kwargs, `timeout`, `key`, and `namespace`, are optional.

    `timeout` can be either an int, or a timedelta (or None).

    `key` is used to create a key for the cached value. It is processed and 
    serialized by CacheCow's `make_key`, so please refer to its documentation.
    
    However, if `key` is None, we'll automatically and determinisically create a
    uniquely identifying key for the function which is hopefully human-readable.

    Additionally, if `key` is callable, or if it's an iterable containing any 
    callables, they will be called with the same args and kwargs as the 
    decorated function, and their return values will be serialized and added to 
    the key.

    `namespace` is used as an alternative way to invalidate a key or a group of
    keys. When `namespace` is used, all keys that belong to the given namespace
    can be invalidated simply by passing the namespace name to
    `invalidate_namespace`. This is especially helpful when you start worker 
    processes that don't know what's already in the cache, since it relieves 
    you of needing to keep track of every key you set if you want to invalidate 
    a group of them at once.

    If `namespace` is a function, it will be called with the same arguments as 
    the decorated function, and its return value will be passed to `make_key` to
    transform it into the cache key used. Unlike the `key` argument though, 
    `namespace` cannot be an iterable that contains functions inside it.

    Giving a function for `namespace` lets you create namespaces dynamically 
    depending on some of the arguments passed to whatever you're caching. For 
    example, you may want to cache several functions in the same namespace 
    depending on the current user.
    
    Note that any `namespace` functions *must* be deterministic: given the same 
    input arguments, it must always produce the same output.
    '''
    def decorator(func):
        _add_delete_cache_member(func, key=key, namespace=namespace)

        @wraps(func)
        def wrapped(*args, **kwargs):
            key_args = (key
                        or _make_key_args_from_function(func, *args, **kwargs))
            _key = _make_key(key_args, namespace, args, kwargs)

            val = cache.get(_key)
            if val is None:
                val = func(*args, **kwargs)
                _set_cache(_key, val, timeout)
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

def cached_view(timeout=None, key=None, namespace=None, add_user_to_key=False):
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
        _add_delete_cache_member(func, key=key, namespace=namespace)

        @wraps(func)
        def wrapped(request, *args, **kwargs):
            if not _can_cache_request(request):
                return func(request, *args, **kwargs)
            
            key_args = key

            # Default key.
            if not key_args:
                # Don't naively add the `request` arg to the cache key.
                key_args = _make_key_args_from_function(func, *args, **kwargs)
                
                # Only add specific parts of the `request` object to the key.
                key_args.extend(chain.from_iterable(request.GET.items()))
                key_args.append(request.method)

                # Current language.
                key_args.append(translation.get_language())

                # Current site, if available.
                key_args.append(getattr(settings, 'SITE_ID', None))

            try:
                if add_user_to_key and request.user.is_authenticated():
                    key_args.append(request.user.id)
            except AttributeError: # maybe "auth" isn't installed.
                pass
            
            # Serialize the key.
            # Add `request` to `args` since _make_key wants all func args in it.
            _key = _make_key(key_args, namespace, (request,) + args, kwargs)

            val = cache.get(_key)
            if val is None:
                val = func(request, *args, **kwargs)
                
                if _can_cache_response(val):
                    _set_cache(_key, val, timeout)
            return val
        return wrapped
    return decorator



