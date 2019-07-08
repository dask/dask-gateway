import textwrap
from collections import OrderedDict
from keyword import iskeyword


__all__ = ("Options", "String", "Bool", "Integer", "Float", "Select")


class AttrDict(dict):
    """A dict that also allows attribute access for keys"""

    def __getattr__(self, k):
        if k in self:
            return self[k]
        raise AttributeError(k)

    def __dir__(self):
        out = set(type(self))
        out.update(k for k in self if k.isidentifier() and not iskeyword(k))
        return list(out)


class Options(object):
    """A description of cluster options.

    Parameters
    ----------
    *fields : Field
        Zero or more configurable fields.
    handler : callable, optional
        A callable with the signature ``handler(options)``, where ``options``
        is the validated dict of user options. Should return a dict of
        configuration overrides to forward to the cluster manager. If not
        provided, the default will return the options unchanged.
    """

    def __init__(self, *fields, handler=None):
        for f in fields:
            if not isinstance(f, Field):
                raise TypeError(
                    "All fields must by instances of ``Field``, got %r"
                    % type(f).__name__
                )
        self.fields = fields
        self.handler = handler

    def get_specification(self):
        return [f.json_spec() for f in self.fields]

    def parse_options(self, request):
        if not isinstance(request, dict):
            raise TypeError("options must be a dict, got %r" % type(request).__name__)
        # Check for extra fields
        extra = set(request).difference(f.field for f in self.fields)
        if extra:
            raise ValueError("Unknown fields %r" % sorted(extra))
        # Validate and normalize options
        options = {}
        for f in self.fields:
            if f.field in request:
                options[f.field] = f.validate(request[f.field])
            else:
                options[f.field] = f.validate(f.default)
        return options

    def get_configuration(self, options):
        if self.handler is None:
            return options
        return self.handler(AttrDict(options))


_field_doc_template = """\
{description}

Parameters
----------
field : str
    The field name to use. Must be a valid Python variable name. This will
    be the keyword user's use to set this field programmatically (e.g.
    ``"worker_cores"``).
{params}
label : str, optional
    A human readable label that will be used in GUI representations (e.g.
    ``"Worker Cores"``). If not provided, ``field`` will be used.
target : str, optional
    The target parameter to set in the processed options dict. Must be a
    valid Python variable name. If not provided, ``field`` will be used.
"""


def field_doc(description, params):
    def inner(cls):
        cls.__doc__ = _field_doc_template.format(
            description=description,
            params=textwrap.dedent(params.strip("\n")).strip("\n"),
        )
        return cls

    return inner


class Field(object):
    def __init__(self, field, default=None, target=None, label=None):
        self.field = field
        # Validate the default
        self.validate(default)
        self.default = default

        if target is None:
            target = field
        self.target = target

        if label is None:
            label = field
        self.label = label

    def json_spec(self):
        out = {"field": self.field, "label": self.label, "default": self.default}
        out["spec"] = self.json_type_spec()
        return out

    def json_type_spec(self):
        raise NotImplementedError

    def validate(self, x):
        raise NotImplementedError


@field_doc(
    description="A string option field.",
    params="""
    default : str, optional
        The default value. Default is the empty string (``""``).
    """,
)
class String(Field):
    def __init__(self, field, default="", label=None, target=None):
        super().__init__(field, default=default, label=label, target=target)

    def validate(self, x):
        if not isinstance(x, str):
            raise TypeError("%s must be a string, got %r" % (self.field, x))
        return x

    def json_type_spec(self):
        return {"type": "string"}


@field_doc(
    description="A boolean option field.",
    params="""
    default : bool, optional
        The default value. Default is False.
    """,
)
class Bool(Field):
    def __init__(self, field, default=False, label=None, target=None):
        super().__init__(field, default=default, label=label, target=target)

    def validate(self, x):
        if not isinstance(x, bool):
            raise TypeError("%s must be a bool, got %r" % (self.field, x))
        return x

    def json_type_spec(self):
        return {"type": "bool"}


class Number(Field):
    def __init__(self, field, default=0, min=None, max=None, label=None, target=None):
        # Temporarily set to allow `validate` to work
        self.min = self.max = None
        if min is not None:
            self.min = self.validate(min)
        if max is not None:
            self.max = self.validate(max)
        super().__init__(field, default=default, label=label, target=target)

    def validate(self, x):
        if self.min is not None and x < self.min:
            raise ValueError("%s must be >= %f, got %s" % (self.field, self.min, x))
        if self.max is not None and x > self.max:
            raise ValueError("%s must be <= %f, got %s" % (self.field, self.max, x))
        return x


@field_doc(
    description="An integer option field.",
    params="""
    default : int, optional
        The default value. Default is 0.
    min : int, optional
        The minimum valid value (inclusive). Unbounded if not set.
    max : int, optional
        The maximum valid value (inclusive). Unbounded if not set.
    """,
)
class Integer(Number):
    def validate(self, x):
        if not isinstance(x, int):
            raise TypeError("%s must be an integer, got %r" % (self.field, x))
        return super().validate(x)

    def json_type_spec(self):
        return {"type": "int", "min": self.min, "max": self.max}


@field_doc(
    description="A float option field.",
    params="""
    default : float, optional
        The default value. Default is 0.
    min : float, optional
        The minimum valid value (inclusive). Unbounded if not set.
    max : float, optional
        The maximum valid value (inclusive). Unbounded if not set.
    """,
)
class Float(Number):
    def validate(self, x):
        if isinstance(x, int):
            x = float(x)
        if not isinstance(x, float):
            raise TypeError("%s must be a float, got %r" % (self.field, x))
        return super().validate(x)

    def json_type_spec(self):
        return {"type": "float", "min": self.min, "max": self.max}


@field_doc(
    description="An option field asking users to select between a few choices.",
    params="""
    options : list
        A list of valid options. Elements may be a tuple of ``(key, value)``,
        or just ``key`` (in which case the value is the same as the key).
        Values may be any Python object, keys must be strings.
    default : str, optional
        The key for the default option. Defaults to the first listed option.
    """,
)
class Select(Field):
    def __init__(self, field, options, default=None, label=None, target=None):
        if not isinstance(options, list):
            raise TypeError("options must be a list")
        options_map = OrderedDict()
        for value in options:
            if isinstance(value, tuple):
                key, value = value
            else:
                key = str(value)
            if not isinstance(key, str):
                raise TypeError("Select keys must be strings, got %r" % key)
            options_map[key] = value

        if default is None:
            default = list(options_map)[0]

        self.options = options_map
        super().__init__(field, default=default, label=label, target=target)

    def validate(self, x):
        if not isinstance(x, str):
            raise TypeError("%s must be a string, got %r" % (self.field, x))
        try:
            return self.options[x]
        except KeyError:
            raise ValueError("%r is not a valid option for %s" % (x, self.field))

    def json_type_spec(self):
        return {"type": "select", "options": list(self.options)}
