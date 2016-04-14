from pprint import pformat

from six import viewkeys
from six.moves import map

from zipline.utils.sentinel import sentinel


def mapall(funcs, seq):
    """
    Parameters
    ----------
    funcs : iterable[function]
        Sequence of functions to map over `seq`.
    seq : iterable
        Sequence over which to map funcs.

    Yields
    ------
    elem : object
        Concatenated result of mapping each ``func`` over ``seq``.

    Example
    -------
    >>> list(mapall([lambda x: x + 1, lambda x: x - 1], [1, 2, 3]))
    [2, 3, 4, 0, 1, 2]
    """
    for func in funcs:
        for elem in seq:
            yield func(elem)


def same(*values):
    """
    Check if all values in a sequence are equal.

    Returns True on empty sequences.

    Example
    -------
    >>> same(1, 1, 1, 1)
    True
    >>> same(1, 2, 1)
    False
    >>> same()
    True
    """
    if not values:
        return True
    first, rest = values[0], values[1:]
    return all(value == first for value in rest)


def _format_unequal_keys(dicts):
    return pformat([sorted(d.keys()) for d in dicts])


def dzip_exact(*dicts):
    """
    Parameters
    ----------
    *dicts : iterable[dict]
        A sequence of dicts all sharing the same keys.

    Returns
    -------
    zipped : dict
        A dict whose keys are the union of all keys in *dicts, and whose values
        are tuples of length len(dicts) containing the result of looking up
        each key in each dict.

    Raises
    ------
    ValueError
        If dicts don't all have the same keys.

    Example
    -------
    >>> result = dzip_exact({'a': 1, 'b': 2}, {'a': 3, 'b': 4})
    >>> result == {'a': (1, 3), 'b': (2, 4)}
    True
    """
    if not same(*map(viewkeys, dicts)):
        raise ValueError(
            "dict keys not all equal:\n\n%s" % _format_unequal_keys(dicts)
        )
    return {k: tuple(d[k] for d in dicts) for k in dicts[0]}


_no_default = sentinel('_no_default')


def getattrs(value, attrs, default=_no_default):
    """
    Perform a chained application of ``getattr`` on ``value`` with the values
    in ``attrs``.

    If ``default`` is supplied, return it if any of the attribute lookups fail.

    Parameters
    ----------
    value : object
        Root of the lookup chain.
    attrs : iterable[str]
        Sequence of attributes to look up.
    default : object, optional
        Value to return if any of the lookups fail.

    Returns
    -------
    result : object
        Result of the lookup sequence.

    Example
    -------
    >>> class EmptyObject(object):
    ...     pass
    ...
    >>> obj = EmptyObject()
    >>> obj.foo = EmptyObject()
    >>> obj.foo.bar = "value"
    >>> getattrs(obj, ('foo', 'bar'))
    'value'

    >>> getattrs(obj, ('foo', 'buzz'))
    Traceback (most recent call last):
       ...
    AttributeError: 'EmptyObject' object has no attribute 'buzz'

    >>> getattrs(obj, ('foo', 'buzz'), 'default')
    'default'
    """
    try:
        for attr in attrs:
            value = getattr(value, attr)
    except AttributeError:
        if default is _no_default:
            raise
        value = default
    return value
