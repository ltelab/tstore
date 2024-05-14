"""Module defining the abstract base class for dataframe tstore wrappers."""

from abc import ABC, abstractmethod
from pathlib import Path
from types import MethodType
from typing import Callable, Union

from tstore.backend import DataFrame


class TSWrapper(ABC):
    """Abstract base class for dataframe tstore wrappers."""

    def __init__(self, df: DataFrame):
        # Set attributes using __dict__ to not trigger __setattr__
        self.__dict__["_df"] = df
        self.__dict__["_dtype"] = type(df)

    @abstractmethod
    def to_tstore(self, *args, **kwargs) -> None:
        """Write the wrapped dataframe as a tstore structure."""

    @staticmethod
    @abstractmethod
    def from_tstore(base_dir: Union[str, Path], *args, **kwargs) -> "TSWrapper":
        """Read a TStore file structure as a TSWrapper around a dataframe."""

    @classmethod
    def wrap(cls, df: DataFrame):
        """Wrap a DataFrame in the appropriate TSWrapper subclass."""
        return cls(df)

    def __getattr__(self, key):
        """Delegate attribute access to the wrapped dataframe."""
        if key in self.__dict__:
            return self.__dict__[key]

        return rewrap_result(getattr(self._df, key), self)

    def __setattr__(self, key, value):
        """Delegate attribute setting to the wrapped dataframe."""
        if key in self.__dict__:
            self.__dict__[key] = value

        else:
            setattr(self._df, key, value)

    def __delattr__(self, key):
        """Delegate attribute deletion to the wrapped dataframe."""
        if key in self.__dict__:
            raise AttributeError(f"Cannot delete attribute {key}.")

        delattr(self._df, key)

    def __getitem__(self, key):
        """Delegate item access to the wrapped dataframe."""
        return rewrap_result(self._df[key], self)

    def __setitem__(self, key, value):
        """Delegate item setting to the wrapped dataframe."""
        self._df[key] = value

    def __delitem__(self, key):
        """Delegate item deletion to the wrapped dataframe."""
        del self._df[key]

    def __repr__(self):
        """Return a string representation of the object and the wrapped dataframe."""
        dtype_path = f"{self._dtype.__module__}.{self._dtype.__qualname__}"
        return f"{self.__class__.__name__} wrapping a {dtype_path}:\n{self._df!s}"

    def __dir__(self):
        """Return the attributes of the object and the wrapped dataframe."""
        own_attrs = set(dir(type(self))) | set(self.__dict__.keys())
        df_attrs = set(dir(self._df))
        return list(own_attrs | df_attrs)


def rewrap_result(obj, tswrapper: TSWrapper):
    """Rewrap the output of a method call if it is a dataframe."""
    if isinstance(obj, tswrapper._dtype):
        return tswrapper.__class__(obj)

    if hasattr(obj, "__getitem__"):
        obj = wrap_indexable_result(obj, tswrapper)

    if isinstance(obj, MethodType):
        obj = wrap_method_result(obj, tswrapper)

    elif callable(obj):
        obj = wrap_callable_result(obj, tswrapper)

    return obj


def wrap_indexable_result(obj, tswrapper: TSWrapper) -> Callable:
    """Wrap the output of __getitem__ if it is a dataframe."""

    class Wrapped(type(obj)):
        def __getitem__(self, key):
            result = super().__getitem__(key)
            return rewrap_result(result, tswrapper)

    try:
        obj.__class__ = Wrapped
    except TypeError:
        # If class cannot be reassigned, create a new instance
        obj = Wrapped(obj)

    return obj


def wrap_method_result(obj, tswrapper: TSWrapper) -> Callable:
    """Wrap the output of a method if it is a dataframe."""

    def wrapped_method(*args, **kwargs):
        result = obj(*args, **kwargs)
        return rewrap_result(result, tswrapper)

    return wrapped_method


def wrap_callable_result(obj: Callable, tswrapper: TSWrapper) -> Callable:
    """Wrap the output of a callable if it is a dataframe."""

    class Wrapped(type(obj)):  # Does not work for methods
        def __call__(self, *args, **kwargs):
            result = super().__call__(*args, **kwargs)
            return rewrap_result(result, tswrapper)

    try:
        obj.__class__ = Wrapped
    except TypeError:
        # If class cannot be reassigned, create a new instance
        obj = Wrapped(obj)

    return obj
