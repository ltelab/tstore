"""Module defining the abstract base class for dataframe tstore wrappers."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Union

from tstore.backend import DataFrame


def rewrap(func: Callable) -> Callable:
    """Decorator to rewrap the output of a method call if it is a dataframe."""

    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        if isinstance(result, self._dtype):
            return self.__class__(result)
        return result

    return wrapper


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

    @rewrap
    def __getattr__(self, key):
        """Delegate attribute access to the wrapped dataframe."""
        if key in self.__dict__:
            return self.__dict__[key]

        return getattr(self._df, key)

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

    @rewrap
    def __getitem__(self, key):
        """Delegate item access to the wrapped dataframe."""
        return self._df[key]

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
