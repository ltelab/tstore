"""Module defining the abstract base class for dataframe tstore wrappers."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Union

import pandas as pd

from tstore.backend import Backend, DataFrame, change_backend


class Proxy:
    """Class wrapping an object and delegating attribute access to it."""

    def __init__(self, obj):
        # Set attributes using __dict__ to not trigger __setattr__
        self.__dict__["_obj"] = obj

    def __getattr__(self, key):
        """Delegate attribute access to the wrapped object."""
        if key in self.__dict__:
            return self.__dict__[key]

        return getattr(self._obj, key)

    def __setattr__(self, key, value):
        """Delegate attribute setting to the wrapped object."""
        if key in self.__dict__:
            self.__dict__[key] = value

        else:
            setattr(self._obj, key, value)

    def __delattr__(self, key):
        """Delegate attribute deletion to the wrapped object."""
        if key in self.__dict__:
            raise AttributeError(f"Cannot delete attribute {key}.")

        delattr(self._obj, key)

    def __getitem__(self, key):
        """Delegate item access to the wrapped object."""
        return self._obj[key]

    def __setitem__(self, key, value):
        """Delegate item setting to the wrapped object."""
        self._obj[key] = value

    def __delitem__(self, key):
        """Delegate item deletion to the wrapped object."""
        del self._obj[key]

    def __call__(self, *args, **kwargs):
        """Delegate call to the wrapped object."""
        return self._obj(*args, **kwargs)

    def __repr__(self):
        """Return a string representation of the object and the wrapped object."""
        obj_type = type(self._obj)
        type_path = f"{obj_type.__module__}.{obj_type.__qualname__}"
        return f"{self.__class__.__name__} wrapping a {type_path}:\n{self._obj}"

    def __dir__(self):
        """Return the attributes of the object and the wrapped object."""
        own_attrs = set(dir(type(self))) | set(self.__dict__.keys())
        df_attrs = set(dir(self._obj))
        return list(own_attrs | df_attrs)


class TSWrapper(ABC, Proxy):
    """Abstract base class for dataframe tstore wrappers.

    Added attributes should be prefixed with `_tstore_` and added to the `__dict__` in the `__init__` method.
    """

    def __init__(self, df: DataFrame):
        super().__init__(df)

    @abstractmethod
    def to_tstore(self, *args, **kwargs) -> None:
        """Write the wrapped dataframe as a tstore structure."""

    @staticmethod
    @abstractmethod
    def from_tstore(base_dir: Union[str, Path], *args, **kwargs) -> "TSWrapper":
        """Read a TStore file structure as a TSWrapper around a dataframe."""

    def change_backend(self, new_backend: Backend) -> "TSWrapper":
        """Return a new wrapper with the dataframe converted to a different backend."""
        new_df = change_backend(self._obj, new_backend)
        kwargs = {
            key.removeprefix("_tstore_"): value for key, value in self.__dict__.items() if key.startswith("_tstore_")
        }
        return self.wrap(new_df, **kwargs)

    @classmethod
    def wrap(cls, df: DataFrame, *args, **kwargs):
        """Wrap a DataFrame in the appropriate TSWrapper subclass."""
        return cls(df, *args, **kwargs)

    def __getattr__(self, key):
        """Delegate attribute access to the wrapped dataframe."""
        if key in self.__dict__:
            return self.__dict__[key]

        return rewrap_result(getattr(self._obj, key), self)

    def __getitem__(self, key):
        """Delegate item access to the wrapped dataframe."""
        return rewrap_result(self._obj[key], self)

    def __call__(self, *args, **kwargs):
        """Delegate call to the wrapped dataframe."""
        return rewrap_result(self._obj(*args, **kwargs), self)


def rewrap_result(obj, tswrapper: TSWrapper):
    """Rewrap the output of a method call if it is a dataframe."""
    if isinstance(obj, type(tswrapper._obj)):
        return tswrapper.__class__(obj)

    if hasattr(obj, "__getitem__"):
        obj = wrap_indexable_result(obj, tswrapper)

    if callable(obj):
        obj = wrap_callable_result(obj, tswrapper)

    return obj


def wrap_indexable_result(obj, tswrapper: TSWrapper) -> Proxy:
    """Wrap the output of __getitem__ if it is a dataframe."""
    # TODO: Generalize for other indexers
    if not isinstance(obj, pd.core.indexing._iLocIndexer):
        return obj

    class IndexWrapper(Proxy):
        def __getitem__(self, key):
            return rewrap_result(self._obj[key], tswrapper)

    return IndexWrapper(obj)


def wrap_callable_result(obj: Callable, tswrapper: TSWrapper) -> Callable:
    """Wrap the output of a callable if it is a dataframe."""

    class CallableWrapper(Proxy):  # Does not work for methods
        def __call__(self, *args, **kwargs):
            result = self._obj(*args, **kwargs)
            return rewrap_result(result, tswrapper)

    return CallableWrapper(obj)
