# -*- coding: utf-8 -*-

from django.conf import settings
from django.test import TestCase
from datetime import timedelta
from cachecow.cache import (make_key, cached_function, _format_key_arg,
                            _make_keys_from_function, _timedelta_to_seconds,
                            invalidate_namespace)
from cachecow.intpacker import pack_int, unpack_int


class CacheHelperTest(TestCase):
    def test_short_key(self):
        args = ['foo', 'bar', 'b1z', 'qu ux']
        key = make_key(*args)
        for arg in args:
            self.assertTrue(arg.replace(' ', '') in key)

    def test_no_brackets_in_list_key(self):
        key = make_key(1,2,3,4)
        self.assertTrue('[' not in key)

    def test_long_key(self):
        args = range(2000)
        key = make_key(*args)
        self.assertTrue(len(key) <= 250)
        self.assertTrue('1.2.3.4.5.6.7.8.9.10' not in key, 'key is not hashed')

    def test_function_decorator_and_cache_deletion(self):
        foo = 10
        
        @cached_function()
        def my_func():
            return foo

        ret = my_func()
        self.assertEqual(ret, foo)

        foo = 20
        self.assertNotEqual(my_func(), foo)

        my_func.delete_cache()
        self.assertEqual(my_func(), foo)

    def test_method_decorator(self):
        foo = 10
        
        class Baz(object):
            @cached_function()
            def my_func(self):
                return foo

        bar = Baz()
        self.assertEqual(bar.my_func(), foo)
        foo = 20
        self.assertNotEqual(bar.my_func(), foo)

    def test_property_decorator(self):
        foo = 77
        
        class Quuuux(object):
            @property
            @cached_function()
            def my_prop(self):
                return foo

        q = Quuuux()
        self.assertEqual(q.my_prop, foo)
        foo = 20
        self.assertNotEqual(q.my_prop, foo)

    def test_key_arg_formatter(self):
        s = _format_key_arg({1:2})
        self.assertTrue('1' in s)
        self.assertTrue('2' in s)
        self.assertNotEqual(s, '{1: 2}')
        s = _format_key_arg('foo bar\t')
        self.assertEqual(s, 'foobar')

    def test_distinct_key_generation(self):
        foo = 50

        @cached_function()
        def my_func2():
            return foo

        a = my_func2()
        foo = 60

        class Bar2(object):
            @cached_function()
            def my_func(self):
                return foo

        self.assertNotEqual(a, Bar2().my_func())

    def test_timedelta_to_s(self):
        t = timedelta(days=2)
        s = _timedelta_to_seconds(t)
        self.assertEqual(s, 3600*48)

    def test_simple_namespaced_key(self):
        factor = 2

        @cached_function(namespace='foospace')
        def my_func(bar):
            return bar * factor

        @cached_function(namespace='foospace')
        def my_other_func(bar):
            return bar % factor

        ret = my_func(4)
        ret2 = my_other_func(4)
        self.assertEqual(ret, 4 * factor)
        self.assertEqual(ret2, 4 % factor)

        # Haven't invalidated yet, so changing `factor` shouldn't matter.
        old_factor, factor = factor, 3
        ret = my_func(4)
        self.assertEqual(ret, 4 * old_factor)
        
        invalidate_namespace('foospace')
        ret = my_func(4)
        ret2 = my_other_func(4)
        self.assertEqual(ret, 4 * factor)
        self.assertEqual(ret2, 4 % factor)

    def test_invalidating_nonexistent_namespace(self):
        invalidate_namespace('nonexistent_namespace')

    def test_namespace_funcs(self):
        for ns_key in ('huh',
                       ('my', 'namespace', 'names',),
                       12345,):
            def ns_func(foo):
                return (foo, ns_key,)
            val = 5
            @cached_function(namespace=ns_func)
            def my_nsed_func(foo):
                return val + foo
            self.assertEqual(my_nsed_func(1), val + 1)
            old_val, val = val, 10
            self.assertEqual(my_nsed_func(1), old_val + 1)
            invalidate_namespace(ns_func(1))
            self.assertEqual(my_nsed_func(1), val + 1)

    def test_namespace_func_kwargs(self):
        def ns_func(foo=None):
            self.assertTrue(foo is not None)
            return ['test_namespace_func_kwargs', foo]
        val = 6
        @cached_function(namespace=ns_func)
        def a_func(foo=None):
            return val + foo
        ret = a_func(foo=4)
        self.assertEqual(ret, 10)

    def test_callable_keys(self):
        thing = {'name': 'bob', 'age': 30}

        # To make sure `thing_key` gets called.
        self._test_func_keys_key_call_count = 0

        def thing_key(p):
            self._test_func_keys_key_call_count += 1
            return p['name']

        @cached_function(keys=['test_lambda_keys', thing_key])
        def get_age(p):
            return p['age']

        self.assertEqual(get_age(thing), thing['age'])
        thing['age'] += 1
        self.assertNotEqual(get_age(thing), thing['age'])
        thing['name'] = 'john' # Changes the cache key, so it will expire
        self.assertEqual(get_age(thing), thing['age'])
        self.assertTrue(self._test_func_keys_key_call_count)



#class CachedViewTest(TestCase):
    
            


class IntPackerTest(TestCase):
    def test_int_packer(self):
        self.assertEqual(pack_int(0), 'A')
        for i in [-200,-10,-5,-4, -3, -2, -1, 0]:
            pack_int(i)

