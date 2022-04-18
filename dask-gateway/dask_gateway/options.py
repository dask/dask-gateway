import weakref
from collections import OrderedDict
from collections.abc import MutableMapping, Sequence
from keyword import iskeyword

import yaml

__all__ = ("Options",)


class Options(MutableMapping):
    """Options to submit to dask-gateway.

    A mutable-mapping that describes all cluster options available for user's
    to set. Options can be modified programatically, or via a widget when using
    a web interface (e.g. Jupyter Notebooks) with ``ipywidgets`` installed.

    Examples
    --------
    Options objects are normally created via the ``Gateway.cluster_options``
    method:

    >>> options = gateway.cluster_options()  # doctest: +SKIP

    Available options can then be accessed or set via attribute or key:

    >>> options.worker_cores  # doctest: +SKIP
    1
    >>> options["worker_cores"]  # doctest: +SKIP
    1
    >>> options.worker_cores = 2  # doctest: +SKIP
    >>> options.worker_cores  # doctest: +SKIP
    2

    Accessing invalid options error appropriately:

    >>> options.not_a_valid_option = 'myvalue'  # doctest: +SKIP
    Traceback (most recent call last):
        ...
    AttributeError: No option 'not_a_valid_option' available

    Errors are also raised if values being set are invalid (e.g. wrong type,
    out-of-bounds, etc...).
    """

    def __init__(self, *fields):
        object.__setattr__(self, "_fields", OrderedDict((f.field, f) for f in fields))

    @classmethod
    def _from_spec(cls, spec):
        return cls(*(Field._from_spec(s) for s in spec))

    def _widget(self):
        if not hasattr(self, "_cached_widget"):
            try:
                import ipywidgets

                title = ipywidgets.HTML("<h2>Cluster Options</h2>")
                children = [item for f in self._fields.values() for item in f.widget()]
                form = ipywidgets.GridBox(
                    children=children,
                    layout=ipywidgets.Layout(
                        justify_content="flex-start",
                        grid_template_columns="auto auto auto",
                        grid_gap="5px 5px",
                    ),
                )
                widget = ipywidgets.VBox(children=[title, form])
            except ImportError:
                widget = None
            object.__setattr__(self, "_cached_widget", widget)
        return self._cached_widget

    def _ipython_display_(self, **kwargs):
        widget = self._widget()
        if widget is not None:
            return widget._ipython_display_(**kwargs)
        from IPython.lib.pretty import pprint

        pprint(self)

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text("Options<...>")
        else:
            with p.group(8, "Options<", ">"):
                for idx, field in enumerate(self._fields.values()):
                    if idx:
                        p.text(",")
                        p.breakable()
                    p.text("%s=" % field.field)
                    p.pretty(field.value)

    def _get(self, key, exc_cls):
        try:
            return self._fields[key].get()
        except KeyError:
            raise exc_cls("No option %r available" % key) from None

    def _set(self, key, value, exc_cls):
        try:
            self._fields[key].set(value)
        except KeyError:
            raise exc_cls("No option %r available" % key) from None

    def __getattr__(self, key):
        return self._get(key, AttributeError)

    def __setattr__(self, key, value):
        return self._set(key, value, AttributeError)

    def __getitem__(self, key):
        return self._get(key, KeyError)

    def __setitem__(self, key, value):
        return self._set(key, value, KeyError)

    def __delitem__(self, key):
        raise TypeError("Cannot delete fields")

    __delattr__ = __delitem__

    def __iter__(self):
        return iter(self._fields)

    def __len__(self):
        return len(self._fields)

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(f for f in self._fields if f.isidentifier() and not iskeyword(f))
        return list(o)


def register_field_type(name):
    def inner(cls):
        Field._field_types[name] = cls
        return cls

    return inner


class Field:
    """A single option field"""

    _field_types = {}

    def __init__(self, field, default, label=None):
        self.field = field
        self.value = self.validate(default)
        self.label = label or field

        self._widgets = weakref.WeakSet()

    @classmethod
    def _from_spec(cls, spec):
        field = spec["field"]
        default = spec["default"]
        label = spec["label"]
        type_spec = dict(spec["spec"])
        typ = type_spec.pop("type")
        return cls._field_types[typ](field, default, label=label, **type_spec)

    def validate(self, x):
        raise NotImplementedError

    def set(self, value):
        self.value = self.validate(value)
        # Update all linked widgets
        for w in self._widgets:
            w.value = self.value

    def get(self):
        return self.value

    def widget(self):
        import ipywidgets

        def handler(change):
            self.set(change.new)

        label = ipywidgets.HTML("<p style='font-weight: bold'>%s:</p>" % self.label)

        input = self._widget()
        input.observe(handler, "value")
        self._widgets.add(input)

        status = ipywidgets.HTML("")

        return label, input, status


@register_field_type("string")
class String(Field):
    """A string option field"""

    def validate(self, x):
        if not isinstance(x, str):
            raise TypeError(f"{self.field} must be a string, got {x!r}")
        return x

    def _widget(self):
        import ipywidgets

        return ipywidgets.Text(value=self.value, continuous_update=False)


@register_field_type("bool")
class Bool(Field):
    """A boolean option field"""

    def validate(self, x):
        if not isinstance(x, bool):
            raise TypeError(f"{self.field} must be a bool, got {x!r}")
        return x

    def _widget(self):
        import ipywidgets

        return ipywidgets.Checkbox(value=self.value, indent=False)


class Number(Field):
    def __init__(self, field, default, label=None, min=None, max=None):
        # Temporarily set to allow `validate` to work
        self.min = self.max = None
        if min is not None:
            self.min = self.validate(min)
        if max is not None:
            self.max = self.validate(max)
        super().__init__(field, default, label=label)

    def validate(self, x):
        if self.min is not None and x < self.min:
            raise ValueError(f"{self.field} must be >= {self.min:f}, got {x}")
        if self.max is not None and x > self.max:
            raise ValueError(f"{self.field} must be <= {self.max:f}, got {x}")
        return x


@register_field_type("int")
class Integer(Number):
    """An integer option field"""

    def validate(self, x):
        if not isinstance(x, int):
            raise TypeError(f"{self.field} must be an integer, got {x!r}")
        return super().validate(x)

    def _widget(self):
        import ipywidgets

        if self.min is None and self.max is None:
            return ipywidgets.IntText(value=self.value)
        else:
            return ipywidgets.BoundedIntText(
                value=self.value,
                min=-(2**63) if self.min is None else self.min,
                max=(2**63 - 1) if self.max is None else self.max,
            )


@register_field_type("float")
class Float(Number):
    """A float option field"""

    def validate(self, x):
        if isinstance(x, int):
            x = float(x)
        if not isinstance(x, float):
            raise TypeError(f"{self.field} must be a float, got {x!r}")
        return super().validate(x)

    def _widget(self):
        import ipywidgets

        if self.min is None and self.max is None:
            return ipywidgets.FloatText(value=self.value)
        else:
            return ipywidgets.BoundedFloatText(
                value=self.value,
                min=self.min or float("-inf"),
                max=self.max or float("inf"),
            )


@register_field_type("select")
class Select(Field):
    """A select option field"""

    def __init__(self, field, default, label=None, options=None):
        if not isinstance(options, Sequence):
            raise TypeError("options must be a sequence")
        elif not len(options):
            raise ValueError("There must be at least one option")
        for o in options:
            if not isinstance(o, str):
                raise TypeError("Select options must be strings, got %r" % o)
        self.options = tuple(options)
        self._options_set = set(options)
        super().__init__(field, default, label=label)

    def validate(self, x):
        if not isinstance(x, str):
            raise TypeError(f"{self.field} must be a string, got {x!r}")
        if x not in self._options_set:
            raise ValueError(f"{x!r} is not a valid option for {self.field}")
        return x

    def _widget(self):
        import ipywidgets

        return ipywidgets.Dropdown(value=self.value, options=self.options)


@register_field_type("mapping")
class Mapping(Field):
    """A mapping option field"""

    def validate(self, x):
        if not isinstance(x, dict):
            raise TypeError(f"{self.field} must be a dict, got {type(x).__name__!r}")
        return x

    def transform(self, value):
        if not value:
            return ""
        try:
            return yaml.safe_dump(value)
        except Exception:
            # Yes this says json, and the check above is with yaml. Since the
            # client->server interactions take place via json, and only widget
            # users will see yaml things, we error with "json" instead.
            raise ValueError("%s must be json serializable" % self.field) from None

    def set(self, value):
        value = self.validate(value)
        text = self.transform(value)
        self.value = value
        # Update all linked widgets
        for w in self._widgets:
            w.value = text

    def widget(self):
        import ipywidgets

        def handler(change):
            tooltip = None
            try:
                text = change.new.strip()
                data = yaml.safe_load(text) if text else {}
                try:
                    self.set(data)
                except Exception as exc:
                    tooltip = str(exc)
            except Exception:
                tooltip = "Invalid YAML"
            if tooltip is None:
                status.value = ""
            else:
                # We have do all styling inline. Since css selectors aren't
                # available for inline css, we rely on javascript instead. This
                # works well enough.
                status.value = """
                <div
                    onMouseEnter="this.children[0].style.visibility = 'visible'"
                    onMouseLeave="this.children[0].style.visibility = 'hidden'"
                > &#10060
                <span
                style="visibility:hidden;z-index:1;font-size:0.8em;padding:2px;position:relative;display:inline-block"
                >
                {tooltip}
                </span>
                </div>
                """.format(
                    tooltip=tooltip
                )

        label = ipywidgets.HTML("<p style='font-weight: bold'>%s:</p>" % self.label)

        input = ipywidgets.Textarea(
            value=self.transform(self.value),
            continuous_update=False,
            placeholder="Enter yaml or json...",
        )
        input.observe(handler, "value")
        self._widgets.add(input)

        status = ipywidgets.HTML("")

        return label, input, status
