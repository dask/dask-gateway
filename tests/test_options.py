import pytest
import yaml

import dask_gateway.options as client_options
import dask_gateway_server.options as server_options
from dask_gateway_server.options import FrozenAttrDict
from dask_gateway_server.models import User


def test_string():
    field = "field_name"
    default = "default_value"
    target = "target_name"
    label = "Field Name"

    # Default values
    s_opt = server_options.String(field)
    assert s_opt.default == ""
    assert s_opt.label == field
    assert s_opt.target == field

    # Specified values propogate
    s_opt = server_options.String(field, default=default, target=target, label=label)
    assert s_opt.default == default
    assert s_opt.target == target
    assert s_opt.label == label

    def check(opt):
        assert opt.validate("hello") == "hello"

        with pytest.raises(TypeError):
            opt.validate(1)

    # Server-side validation
    check(s_opt)

    # Serialization works
    spec = s_opt.json_spec()

    c_opt = client_options.Field._from_spec(spec)

    assert isinstance(c_opt, client_options.String)
    assert c_opt.value == default
    assert c_opt.label == label

    # Client-side validation
    check(c_opt)


def test_bool():
    field = "field_name"
    default = True
    target = "target_name"
    label = "Field Name"

    # Default values
    s_opt = server_options.Bool(field)
    assert s_opt.default is False
    assert s_opt.label == field
    assert s_opt.target == field

    # Specified values propogate
    s_opt = server_options.Bool(field, default=default, target=target, label=label)
    assert s_opt.default == default
    assert s_opt.target == target
    assert s_opt.label == label

    def check(opt):
        assert opt.validate(True) is True

        with pytest.raises(TypeError):
            opt.validate(1)

    # Server-side validation
    check(s_opt)

    # Serialization works
    spec = s_opt.json_spec()

    c_opt = client_options.Field._from_spec(spec)

    assert isinstance(c_opt, client_options.Bool)
    assert c_opt.value == default
    assert c_opt.label == label

    # Client-side validation
    check(c_opt)


def test_int():
    field = "field_name"
    default = 10
    target = "target_name"
    label = "Field Name"
    min = 10
    max = 20

    # Default values
    s_opt = server_options.Integer(field)
    assert s_opt.default == 0
    assert s_opt.label == field
    assert s_opt.target == field
    assert s_opt.min is None
    assert s_opt.max is None

    # Specified values propogate
    s_opt = server_options.Integer(
        field, default=default, target=target, label=label, min=min, max=max
    )
    assert s_opt.default == default
    assert s_opt.target == target
    assert s_opt.label == label
    assert s_opt.min == min
    assert s_opt.max == max

    def check(opt):
        assert opt.validate(15) == 15
        assert opt.validate(10) == 10
        assert opt.validate(20) == 20

        with pytest.raises(TypeError):
            opt.validate("1")
        with pytest.raises(ValueError):
            opt.validate(min - 1)
        with pytest.raises(ValueError):
            opt.validate(max + 1)

    # Server-side validation
    check(s_opt)

    # Serialization works
    spec = s_opt.json_spec()

    c_opt = client_options.Field._from_spec(spec)

    assert isinstance(c_opt, client_options.Integer)
    assert c_opt.value == default
    assert c_opt.label == label
    assert s_opt.min == min
    assert s_opt.max == max

    # Client-side validation
    check(c_opt)


def test_float():
    field = "field_name"
    default = 10.0
    target = "target_name"
    label = "Field Name"
    min = 10
    max = 20

    # Default values
    s_opt = server_options.Float(field)
    assert s_opt.default == 0
    assert s_opt.label == field
    assert s_opt.target == field
    assert s_opt.min is None
    assert s_opt.max is None

    # Specified values propogate
    s_opt = server_options.Float(
        field, default=default, target=target, label=label, min=min, max=max
    )
    assert s_opt.default == default
    assert s_opt.target == target
    assert s_opt.label == label
    assert s_opt.min == min
    assert s_opt.max == max
    assert isinstance(s_opt.min, float)
    assert isinstance(s_opt.max, float)
    assert isinstance(s_opt.default, float)

    def check(opt):
        assert opt.validate(15.5) == 15.5
        assert opt.validate(10) == 10.0
        assert opt.validate(20) == 20.0

        with pytest.raises(TypeError):
            opt.validate("1")
        with pytest.raises(ValueError):
            opt.validate(min - 1)
        with pytest.raises(ValueError):
            opt.validate(max + 1)

    # Server-side validation
    check(s_opt)

    # Serialization works
    spec = s_opt.json_spec()

    c_opt = client_options.Field._from_spec(spec)

    assert isinstance(c_opt, client_options.Float)
    assert c_opt.value == default
    assert c_opt.label == label
    assert s_opt.min == min
    assert s_opt.max == max

    # Client-side validation
    check(c_opt)


def test_select():
    field = "field_name"
    default = "banana"
    options = ["apple", "banana", ("orange", 1)]
    target = "target_name"
    label = "Field Name"

    # Default values
    s_opt = server_options.Select(field, options)
    assert s_opt.default == "apple"
    assert s_opt.label == field
    assert s_opt.target == field

    # Specified values propogate
    s_opt = server_options.Select(
        field, options, default=default, target=target, label=label
    )
    assert s_opt.default == default
    assert s_opt.target == target
    assert s_opt.label == label

    def check(opt):
        assert opt.validate("apple") == "apple"
        assert opt.validate("orange") == "orange"

        with pytest.raises(TypeError):
            opt.validate(1)
        with pytest.raises(ValueError):
            opt.validate("grape")

    # Server-side validation
    check(s_opt)

    # Bad server-side constructors
    with pytest.raises(TypeError):
        server_options.Select(field, 1)

    with pytest.raises(TypeError):
        server_options.Select(field, [1, 2, 3])

    with pytest.raises(ValueError):
        server_options.Select(field, [])

    # Serialization works
    spec = s_opt.json_spec()

    c_opt = client_options.Field._from_spec(spec)

    assert isinstance(c_opt, client_options.Select)
    assert c_opt.value == default
    assert c_opt.options == ("apple", "banana", "orange")
    assert c_opt.label == label

    # Client-side validation
    check(c_opt)

    # Bad client-side constructors
    with pytest.raises(TypeError):
        client_options.Select(field, default, options=1)

    with pytest.raises(TypeError):
        client_options.Select(field, default, options=[1, 2, 3])

    with pytest.raises(ValueError):
        client_options.Select(field, default, options=[])


def test_mapping():
    field = "field_name"
    default = {"some": "default"}
    target = "target_name"
    label = "Field Name"

    # Default values
    s_opt = server_options.Mapping(field)
    assert s_opt.default == {}
    assert s_opt.label == field
    assert s_opt.target == field

    # Specified values propogate
    s_opt = server_options.Mapping(field, default=default, target=target, label=label)
    assert s_opt.default is default
    assert s_opt.target == target
    assert s_opt.label == label

    # default is copied when returned to server
    assert s_opt.get_default() == default
    assert s_opt.get_default() is not default

    def check(opt):
        value = {"a": {"nested": "mapping"}, "b": 2}
        assert opt.validate(value) == value

        with pytest.raises(TypeError):
            opt.validate(1)

    # Server-side validation
    check(s_opt)

    # Serialization works
    spec = s_opt.json_spec()

    c_opt = client_options.Field._from_spec(spec)

    assert isinstance(c_opt, client_options.Mapping)
    assert c_opt.value == default
    assert c_opt.label == label

    # Client-side validation
    check(c_opt)


def test_frozen_attr_dict():
    d = {"valid": 1, "not valid": 2, "for": 3}
    ad = FrozenAttrDict(d)

    # getattr
    assert ad.valid == 1
    with pytest.raises(AttributeError):
        ad.missing

    # getitem
    assert ad["not valid"] == 2

    # setattr
    with pytest.raises(Exception):
        ad.valid = 1

    # setitem
    with pytest.raises(TypeError):
        ad["valid"] = 1

    # len
    assert len(ad) == len(d)

    # iter
    assert list(ad) == list(d)

    # coversion
    assert dict(ad) == d

    # dir
    tabs = dir(ad)
    assert "valid" in tabs
    assert "not valid" not in tabs
    assert "for" not in tabs


@pytest.fixture
def server_opts():
    return server_options.Options(
        server_options.Integer("integer_field", 1, target="integer_field2"),
        server_options.Float("float_field", 2.5, min=2, max=4),
        server_options.Select("select_field", [("a", 5), "b", ("c", 10)]),
    )


def test_server_options_get_specification(server_opts):
    spec = server_opts.get_specification()
    c_opts = client_options.Options._from_spec(spec)
    assert set(c_opts) == {"integer_field", "float_field", "select_field"}


def test_server_options_parse_options(server_opts):
    res = server_opts.parse_options({})
    assert res == {"integer_field": 1, "float_field": 2.5, "select_field": "a"}

    res = server_opts.parse_options({"select_field": "c", "float_field": 3})
    assert res == {"integer_field": 1, "float_field": 3.0, "select_field": "c"}

    with pytest.raises(TypeError):
        server_opts.parse_options(None)

    with pytest.raises(ValueError):
        server_opts.parse_options({"extra1": 1, "extra2": 2, "integer_field": 3})


def test_server_options_get_configuration(server_opts):
    options = {"integer_field": 2}
    sol = {"integer_field2": 2, "float_field": 2.5, "select_field": 5}
    user = User("alice")

    # Default handler
    config = server_opts.get_configuration(options, user)
    assert config == sol

    # Custom handler, no user
    def handler(options):
        assert isinstance(options, FrozenAttrDict)
        assert dict(options) == sol
        return {"a": 1}

    server_opts.handler = handler
    assert server_opts.get_configuration(options, user) == {"a": 1}

    # Custom handler, with user
    def handler(options, user):
        assert isinstance(options, FrozenAttrDict)
        assert dict(options) == sol
        return {"a": 1, "username": user.name}

    server_opts.handler = handler
    assert server_opts.get_configuration(options, user) == {"a": 1, "username": "alice"}


@pytest.fixture
def client_opts():
    return client_options.Options(
        client_options.Integer("worker_cores", 1, min=1, max=4, label="Worker Cores"),
        client_options.Float(
            "worker_memory", 1, min=1, max=8, label="Worker Memory (GB)"
        ),
        client_options.Integer("scheduler_cores", 1, label="Scheduler Cores"),
        client_options.Float("scheduler_memory", 1, label="Scheduler Memory (GB)"),
        client_options.Select(
            "environment", "basic", options=["basic", "ml"], label="Conda Environment"
        ),
        client_options.Bool("keep_logs", False, label="Keep Logs"),
        client_options.String("queue", "default", label="Queue"),
        client_options.Mapping(
            "environ", {"default": "vals"}, label="Environment variables"
        ),
    )


def get_widget_for_field(options, field):
    pytest.importorskip("ipywidgets")

    widget = options._widget()
    for n, f in enumerate(options._fields):
        if f == field:
            break
    else:
        raise ValueError("No field %s" % field)
    # This is fragile to the formatting of the widgets
    input = widget.children[1].children[3 * n + 1]
    status = widget.children[1].children[3 * n + 2]
    return input, status


def test_client_options(client_opts):
    # Key and attribute access works
    client_opts.worker_cores = 2
    assert client_opts.worker_cores == 2
    client_opts["worker_cores"] = 1
    assert client_opts.worker_cores == 1

    # Cannot delete options
    with pytest.raises(TypeError):
        del client_opts.worker_cores

    with pytest.raises(TypeError):
        del client_opts["worker_cores"]

    # Cannot get/set unknown options
    with pytest.raises(AttributeError):
        client_opts.unknown
    with pytest.raises(AttributeError):
        client_opts.unknown = 1

    with pytest.raises(KeyError):
        client_opts["unknown"]
    with pytest.raises(KeyError):
        client_opts["unknown"] = 1

    # Values are type checked
    with pytest.raises(TypeError):
        client_opts.worker_cores = 1.5
    assert client_opts.worker_cores == 1

    with pytest.raises(ValueError):
        client_opts.environment = "unknown"
    assert client_opts.environment == "basic"

    # Tab completion works
    tabs = dir(client_opts)
    assert "worker_cores" in tabs
    assert "queue" in tabs

    # Other mapping methods
    assert len(client_opts) == len(client_opts._fields)
    assert len(dict(client_opts)) == len(client_opts._fields)


def test_client_options_pretty_printing(client_opts):
    pretty = pytest.importorskip("IPython.lib.pretty")

    res = pretty.pretty(client_opts)
    assert "worker_cores" in res
    assert "Options<" in res


def test_client_options_widget(client_opts):
    ipywidgets = pytest.importorskip("ipywidgets")

    widget = client_opts._widget()
    assert isinstance(widget, ipywidgets.Box)

    # widget is cached
    assert widget is client_opts._widget()


def check_opt_widget(opts, field, val1, val2, *bad_values):
    widget, _ = get_widget_for_field(opts, field)
    assert widget.value == getattr(opts, field)
    # widget to opts
    widget.value = val1
    assert getattr(opts, field) == val1
    # opts to widget
    setattr(opts, field, val2)
    assert widget.value == val2

    for val in bad_values:
        with pytest.raises(Exception):
            setattr(opts, field, val)
        # Unchanged
        assert getattr(opts, field) == val2
        assert widget.value == val2


def test_client_bool_widget(client_opts):
    check_opt_widget(client_opts, "keep_logs", True, False, "bad value")


def test_client_string_widget(client_opts):
    check_opt_widget(client_opts, "queue", "queue-one", "queue-two", 1)


def test_client_select_widget(client_opts):
    check_opt_widget(client_opts, "environment", "ml", "basic", "unknown", 1)


def test_client_integer_widget(client_opts):
    check_opt_widget(client_opts, "worker_cores", 2, 3, 10, -1, "bad value")
    check_opt_widget(client_opts, "scheduler_cores", 2, 3, "bad value")


def test_client_float_widget(client_opts):
    check_opt_widget(client_opts, "worker_memory", 2.5, 3.5, 100, -1, "bad value")
    check_opt_widget(client_opts, "scheduler_memory", 2.5, 3.5, "bad value")


def test_client_mapping_widget(client_opts):
    pytest.importorskip("ipywidgets")

    textarea, status = get_widget_for_field(client_opts, "environ")

    val1 = {"hello": "world"}
    val2 = {"some": {"nested": "values"}, "other": 2}
    text2 = yaml.safe_dump(val2)
    test_cases = [({}, ""), (val1, yaml.safe_dump(val1)), (val2, text2)]

    # widget to opts
    for val, text in test_cases:
        textarea.value = text
        assert client_opts.environ == val
        assert status.value == ""

    # opts to widget
    for val, text in test_cases:
        client_opts.environ = val
        assert textarea.value == text
        assert status.value == ""

    # Validation in opts
    for val in [None, 1, [1, 2, 3], {"a": object()}]:
        with pytest.raises(Exception):
            client_opts.environ = val
        # Unchanged
        assert client_opts.environ == val2
        assert textarea.value == text2
        assert status.value == ""

    # Validation in widget
    for text in ["{", "1", "[1, 2, 3]"]:
        textarea.value = text
        # Unchanged
        assert client_opts.environ == val2
        assert status.value != ""

    # Resets status once valid again
    textarea.value = "HELLO: WORLD"
    assert client_opts.environ == {"HELLO": "WORLD"}
    assert status.value == ""

    # Whitespace is ignored
    textarea.value = "    "
    assert client_opts.environ == {}
    assert status.value == ""
