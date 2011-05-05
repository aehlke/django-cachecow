# -*- coding: utf-8 -*-

from django.conf import settings
from django.test import TestCase
from datetime import timedelta
from cachecow.cache import (make_key, cached_function, _format_key_arg,
                            _make_keys_from_function, _timedelta_to_seconds,
                            invalidate_namespace)
from cachecow.intpacker import pack_int, unpack_int

print 'test file'

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
        self.assertTrue(
                '1.2.3.4.5.6.7.8.9.10.11.12' not in key, 'key is not hashed')

    def test_function_decorator(self):
        foo = 10
        
        @cached_function()
        def my_func():
            return foo

        ret = my_func()
        self.assertEqual(ret, foo)

        foo = 20
        ret = my_func()
        self.assertNotEqual(ret, foo)

        my_func.delete_cache()
        ret = my_func()
        self.assertEqual(ret, foo)

    def test_method_decorator(self):
        foo = 10
        
        class Baz(object):
            @cached_function()
            def my_func(self):
                return foo

        bar = Baz()
        ret = bar.my_func()
        self.assertEqual(ret, foo)

        foo = 20
        ret = bar.my_func()
        self.assertNotEqual(ret, foo)

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

        b = Bar2().my_func()
        self.assertNotEqual(a, b)

    def test_timedelta_to_s(self):
        t = timedelta(days=2)
        s = _timedelta_to_seconds(t)
        self.assertEqual(s, 3600*48)

    def test_simple_namespaced_key(self):
        factor = 2

        @cached_function(namespace='foospace')
        def my_func(bar):
            return bar * factor

        ret = my_func(4)
        self.assertEqual(ret, 4 * factor)
        
        invalidate_namespace('foospace')
        factor = 3
        ret = my_func(4)
        self.assertEqual(ret, 4 * factor)

    def test_invalidating_nonexistent_namespace(self):
        invalidate_namespace('nonexistent_namespace')



class IntPackerTest(TestCase):
    def test_int_packer(self):
        self.assertEqual(pack_int(0), 'A')
        for i in [-200,-10,-5,-4, -3, -2, -1, 0]:
            pack_int(i)

