from functools import wraps
import inspect
from itertools import chain
import logging

from django.conf import settings
from django.contrib import messages
from django.core.cache import cache
from django.http import HttpRequest
from django.utils import translation

from cachecow.cache import set_cache, make_key, key_arg_iterator
from cachecow.intpacker import pack_int


logger = logging.getLogger(__name__)


def _make_key_for_func(key_args, func_args, func_kwargs, namespace=None):
    '''
    Returns the cache key to use for the decorated function. Calls and replaces
    any callable items in `key_args` with their return values before sending 
    `key_args` over to `make_key`. Does the same for a callable `namespace`.
    '''
    def call_if_callable(obj):
        if callable(obj):
            return obj(*func_args, **func_kwargs)
        return obj

    if namespace is not None:
        namespace = call_if_callable(namespace)

    key_args = map(call_if_callable, key_arg_iterator(key_args))

    return make_key(key_args, namespace=namespace)


def _add_delete_cache_member(func, key=None, namespace=None, add_user_to_key=False):
    '''
    Adds a `delete_cache` member function to `func`. Pass it the same args
    as `func` so that it can find the right key to delete.

    If `func` was decorated with `cached_function` or `cached_view` with a
    `key` parameter specified, `delete_cache` takes no arguments.
    '''
    def delete_cache(*args, **kwargs):
        key_args = key or _make_key_args_from_function(func, *args, **kwargs)

        if add_user_to_key and kwargs.get('user') is not None:
            # We can assume that key is specified (see cached_view's docstring).
            key_args = chain(key_arg_iterator(key_args, max_depth=0), kwargs['user'])

        _key = _make_key_for_func(key_args, args, kwargs, namespace=namespace)
        cache.delete(_key)

    func.delete_cache = delete_cache


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
            _key = _make_key_for_func(key_args, args, kwargs,
                                      namespace=namespace)

            val = cache.get(_key)
            if val is None:
                val = func(*args, **kwargs)
                set_cache(_key, val, timeout)
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
    return (response.status_code != 200
            or 'no-cache' in response.get('Cache-Control', '')
            or 'no-cache' in response.get('Pragma', ''))


def cached_view(timeout=None, key=None, namespace=None, add_user_to_key=False,
                request_gatekeeper=_can_cache_request,
                response_gatekeeper=_can_cache_response):
    '''
    Use this instead of `cached_function` for caching views.  See 
    `cached_function` for documentation on how to use this.

    Handles HttpRequest objects intelligently when auto-generating the 
    cache key.

    Only caches GET and HEAD requests which have an HTTP 200 response code.
    Doesn't cache responses which have "Cache-Control: no-cache" or 
    "Pragma: no-cache" in the headers.

    If `add_user_to_key` is True, the key will be prefixed with the logged-in
    user's ID when logged in. Currently this can only be used if `key` is also
    specified, in order to avoid conflicts with function kwargs.
    '''
    if add_user_to_key and key is None:
        raise ValueError("Cannot use add_user_to_key without also specifing key.")

    def decorator(func):
        _add_delete_cache_member(func, key=key, namespace=namespace,
                                 add_user_to_key=add_user_to_key)

        @wraps(func)
        def wrapped(request, *args, **kwargs):
            if not request_gatekeeper(request):
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

            if add_user_to_key and request.user.is_authenticated():
                key_args = chain(key_arg_iterator(key_args, max_depth=0), [request.user.id])

            # Serialize the key.
            # Add `request` to `args` since _make_key wants all func args in it.
            _key = _make_key_for_func(key_args, (request,) + args, kwargs,
                                      namespace=namespace)

            val = cache.get(_key)
            if val is None:
                val = func(request, *args, **kwargs)

                if response_gatekeeper(val):
                    set_cache(_key, val, timeout)
            return val
        return wrapped
    return decorator

