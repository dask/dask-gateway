from collections import OrderedDict
from keyword import iskeyword


__all__ = ("Options", "String", "Bool", "Integer", "Float", "MemoryLimit", "Select")


# A singleton indicating no default value
no_default = type(
    "no_default",
    (object,),
    dict.fromkeys(["__repr__", "__reduce__"], lambda s: "no_default"),
)()


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
            raise ValueError("Unknown fields %r" % extra)
        # Validate and normalize options
        options = {}
        for f in self.fields:
            if f.field in request:
                options[f.field] = f.validate(request[f.field])
            elif f.default is not no_default:
                options[f.field] = f.default
            else:
                raise ValueError("must specify value for %r" % f.field)
        return options

    def get_configuration(self, options):
        if self.handler is None:
            return options
        return self.handler(AttrDict(options))


class Field(object):
    def __init__(self, field, default=no_default, target=None, label=None):
        self.field = field
        if default is not no_default:
            default = self.validate(default)
        self.default = default
        if target is None:
            target = field
        self.target = target
        self.label = label

    def json_spec(self):
        out = {"field": self.field}
        if self.default is not no_default:
            out["default"] = self.default
        if self.label is not None:
            out["label"] = self.label
        out["spec"] = self.json_type_spec()
        return out

    def json_type_spec(self):
        raise NotImplementedError

    def validate(self, x):
        raise NotImplementedError


class String(Field):
    def validate(self, x):
        if not isinstance(x, str):
            raise TypeError("%s must be a string, got %r" % (self.field, x))
        return x

    def json_type_spec(self):
        return {"type": "string"}


class Bool(Field):
    def __init__(self, field, default=False, label=None):
        super().__init__(field, default=default, label=label)

    def validate(self, x):
        if not isinstance(x, bool):
            raise TypeError("%s must be a bool, got %r" % (self.field, x))
        return x

    def json_type_spec(self):
        return {"type": "bool"}


class Number(Field):
    def __init__(self, field, default=no_default, label=None, min=None, max=None):
        # Temporarily set to allow `validate` to work
        self.min = self.max = None
        if min is not None:
            self.min = self.validate(min)
        if max is not None:
            self.max = self.validate(max)
        super().__init__(field, default=default, label=label)

    def validate(self, x):
        if self.min is not None and x < self.min:
            raise ValueError("%s must be >= %f, got %s" % (self.field, self.min, x))
        if self.max is not None and x > self.max:
            raise ValueError("%s must be <= %f, got %s" % (self.field, self.max, x))
        return x


class Integer(Number):
    def validate(self, x):
        if not isinstance(x, int):
            raise TypeError("%s must be an integer, got %r" % (self.field, x))
        return super().validate(x)

    def json_type_spec(self):
        return {"type": "int", "min": self.min, "max": self.max}


class Float(Number):
    def validate(self, x):
        if isinstance(x, int):
            x = float(x)
        if not isinstance(x, float):
            raise TypeError("%s must be a float, got %r" % (self.field, x))
        return super().validate(x)

    def json_type_spec(self):
        return {"type": "float", "min": self.min, "max": self.max}


class MemoryLimit(Integer):
    """A specification of a memory limit, with optional units.

    Supported units are:
      - K -> Kibibytes
      - M -> Mebibytes
      - G -> Gibibytes
      - T -> Tebibytes
    """

    UNIT_SUFFIXES = {"K": 2 ** 10, "M": 2 ** 20, "G": 2 ** 30, "T": 2 ** 40}

    _error_template = (
        "{field} must be a valid memory specification, got {value}. Expected an "
        "int or a string with suffix K, M, G, T"
    )

    def validate(self, value):
        if isinstance(value, (int, float)):
            return int(value)

        try:
            num = float(value[:-1])
        except ValueError:
            raise ValueError(self._error_template.format(field=self.field, value=value))
        suffix = value[-1]

        if suffix not in self.UNIT_SUFFIXES:
            raise ValueError(self._error_template.format(field=self.field, value=value))
        return int(float(num) * self.UNIT_SUFFIXES[suffix])

    def json_type_spec(self):
        return {"type": "memory", "min": self.min, "max": self.max}


class Select(Field):
    def __init__(self, field, options=None, default=no_default, label=None):
        if options is None:
            raise ValueError("Must provide at least one option")
        elif not isinstance(options, list):
            raise TypeError("options must be a list")
        options_map = OrderedDict()
        for value in options:
            if isinstance(value, tuple):
                label, value = value
                label = str(label)
            else:
                label = str(value)
            options_map[label] = value

        if default is not no_default:
            default = str(default)
            if default not in self.options_map:
                raise ValueError("default %r not in options" % default)

        self.options = options_map
        super().__init__(field, default=default, label=label)

    def validate(self, x):
        if not isinstance(x, str):
            raise TypeError("%s must be a string, got %r" % (self.field, x))
        try:
            return self.options[x]
        except KeyError:
            raise ValueError("%r is not a valid option for %s" % (x, self.field))

    def json_type_spec(self):
        return {"type": "select", "options": list(self.options)}
