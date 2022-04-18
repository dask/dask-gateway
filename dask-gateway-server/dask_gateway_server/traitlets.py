from traitlets import Integer, List, TraitError, TraitType
from traitlets import Type as _Type
from traitlets import Unicode
from traitlets.config import Application

# Override default values for logging
Application.log_level.default_value = "INFO"
Application.log_format.default_value = (
    "%(log_color)s[%(levelname)1.1s %(asctime)s.%(msecs).03d "
    "%(name)s]%(reset)s %(message)s"
)


# Adapted from JupyterHub
class MemoryLimit(Integer):
    """A specification of a memory limit, with optional units.

    Supported units are:
      - K -> Kibibytes
      - M -> Mebibytes
      - G -> Gibibytes
      - T -> Tebibytes
    """

    UNIT_SUFFIXES = {"K": 2**10, "M": 2**20, "G": 2**30, "T": 2**40}

    def validate(self, obj, value):
        if isinstance(value, (int, float)):
            return int(value)

        try:
            num = float(value[:-1])
        except ValueError:
            raise TraitError(
                "{val} is not a valid memory specification. Must be an int or "
                "a string with suffix K, M, G, T".format(val=value)
            )
        suffix = value[-1]

        if suffix not in self.UNIT_SUFFIXES:
            raise TraitError(
                "{val} is not a valid memory specification. Must be an int or "
                "a string with suffix K, M, G, T".format(val=value)
            )
        return int(float(num) * self.UNIT_SUFFIXES[suffix])


class Callable(TraitType):
    """A trait which is callable"""

    info_text = "a callable"

    def validate(self, obj, value):
        if callable(value):
            return value
        else:
            self.error(obj, value)


class Type(_Type):
    """An implementation of `Type` with better errors"""

    def validate(self, obj, value):
        if isinstance(value, str):
            try:
                value = self._resolve_string(value)
            except ImportError as exc:
                raise TraitError(
                    "Failed to import %r for trait '%s.%s':\n\n%s"
                    % (value, type(obj).__name__, self.name, exc)
                )
        return super().validate(obj, value)


class Command(List):
    """Traitlet for a command that should be a list of strings,
    but allows it to be specified as a single string.
    """

    def __init__(self, default_value=None, **kwargs):
        kwargs.setdefault("minlen", 1)
        if isinstance(default_value, str):
            default_value = [default_value]
        super().__init__(Unicode(), default_value, **kwargs)

    def validate(self, obj, value):
        if isinstance(value, str):
            value = [value]
        return super().validate(obj, value)
