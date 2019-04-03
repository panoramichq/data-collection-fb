# from Daniel Dotsenko's personal collection of boilerplate :)

from decorator import decorate


MEMOIZE_CACHE_PROPERTY_NAME = '_memoized'


class MemoizeMixin(object):

    def clear_cache(self, **kwargs):
        """
        Allows to clear all or part of memoize cache on the object

        If called with no args, clears all cache.
        If passed with some named args, clears the cache for those named args.
        The value of passed named arg is irrelevant, as only the arg name is used.

        Self is returned for ease of chaining.

        Example::

            class O(MemoizeMixin):
                ...
                @property
                @memoized_property
                def property_a(self):
                    return 'value A'

                @property
                @memoized_property
                def property_b(self):
                    return 'value B'

            o = O()
            o.property_a
            # Output: 'value A'
            o.property_b
            # Output: 'value B'

            o._memoized
            # Output: {"property_a": 'value A', "property_b": 'value B'}

            o.clear_cache(property_a=True)

            o._memoized
            # Output: {"property_b": 'value B'}

            o.property_a
            # Output: 'value A'

            o._memoized
            # Output: {"property_a": 'value A', "property_b": 'value B'}

            o.clear_cache()
            o._memoized
            # Output: {}


        :param dict kwargs:
        :return: Instance of self for pass-through convenience
        """

        cache = getattr(self, MEMOIZE_CACHE_PROPERTY_NAME, {})
        if kwargs:
            # Only some properties are to be removed from cache
            keys = set(kwargs.keys())
        else:
            # all properties are to be removed from cache
            keys = set(cache.keys())

        for key in keys:
            cache.pop(key, None)

        return self # for ease of chaining

    def set_cache(self, **kwargs):
        """
        Allows to prepopulate memoize cache on the object

        Self is returned for ease of chaining.

        Example::

            class O(MemoizeMixin):
                ...
                @property
                @memoized_property
                def property_a(self):
                    return 'value A'

                @property
                @memoized_property
                def property_b(self):
                    return 'value B'

            o = O().set_cache(
                property_a='precached value A',
                property_b='precached value B'
            )

            o._memoized
            # Output: {"property_a": 'precached value A', "property_b": 'precached value B'}

            o.property_a
            # Output: 'precached value A'

        :param kwargs:
        :return:
        """
        cache = getattr(self, MEMOIZE_CACHE_PROPERTY_NAME, {})
        setattr(self, MEMOIZE_CACHE_PROPERTY_NAME, cache)
        cache.update(kwargs)

        return self # for ease of chaining


def _memoized_property(f, self, *args):
    f_name = getattr(f, '__name__')

    # note that we do NOT set the cache on the menthod object,
    # but on the instance of the class the method is exposed on.
    cache = getattr(self, MEMOIZE_CACHE_PROPERTY_NAME, {})
    setattr(self, MEMOIZE_CACHE_PROPERTY_NAME, cache)

    if args:
        # if there are args passed, it must be the setter
        v = args[0]
        f(self, v)  # allow operation to blow up before entering value in the cache
        cache[f_name] = v
    else:
        # if it's not setter, must be getter
        if f_name not in cache:
            cache[f_name] = f(self)  # again, allow operation to blow up before entering value in the cache
        return cache[f_name]


def memoized_property(f):
    """
    Decorator for class property that marks that property for memoization.

    Set on getters AND setters (if declared) to allow cache-busting on set

    @memoized_property automatically detects property method name.
    In the below case - "property_a". It will use that name as key in
    memoization dict.

    As a benefit of using central cache dict, the cache can be preloaded in bulk.

    Example::

        class O(MemoizeMixin):

            @property
            @memoized_property
            def property_a(self):
                return 'some difficult to compute value'

            @property_a.setter
            @memoized_property
            def property_a(self, val):
                some_persist_op(val)

        # classes inheriting from MemoizeMixin expose set_cache method that
        # allows you to pre-pop memoize cache on the object and returns self:
        o = O().set_cache(property_a='value', some_other_prop='another value')


    :param callable f:
    :return: Same callable but with memoize cache object stuck on it
    """

    return decorate(f, _memoized_property)
