#!/usr/bin/env python3
"""
Created on Mon Jun 12 16:23:54 2023.

@author: ghiggi
"""
import re
from typing import Any

from pandas.api.extensions import (
    ExtensionDtype,
    register_extension_dtype,
)

from tstore.tsdf.ts_class import TS

#### Notes
# https://pandas.pydata.org/pandas-docs/stable/reference/extensions.html
# https://pandas.pydata.org/pandas-docs/stable/development/extending.html#extensionarray
# https://pandas.pydata.org/pandas-docs/stable/development/extending.html#extension-types
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.api.extensions.ExtensionArray.html
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.api.extensions.ExtensionDtype.html

# https://github.com/geopandas/geopandas/blob/83ab5c63890a2575b95ede6b8a8ef469753c9605/geopandas/array.py#L33
# https://github.com/geopandas/geopandas/blob/83ab5c63890a2575b95ede6b8a8ef469753c9605/geopandas/array.py#L261
# https://stackoverflow.com/questions/68893521/simple-example-of-pandas-extensionarray

# https://itnext.io/guide-to-pandas-extension-types-and-how-to-create-your-own-3b213d689c86


#### Extension DType


class TSDtype(ExtensionDtype):
    """An ExtensionDtype for TS time series data."""

    # Scalar type for the array
    type = TS

    # Required for all parameterized dtypes
    _metadata = ("ts_class",)
    _match = re.compile(r"TS\[(?P<ts_class>.+)\]")

    def __init__(self, ts_class=None):
        if ts_class is None:
            ts_class = "-"
        self._ts_class = ts_class

    def __str__(self) -> str:
        """String representation."""
        return f"TS[{self._ts_class}]"

    # Required for all ExtensionDtype subclasses
    @property
    def name(self) -> str:
        """A string representation of the dtype."""
        return str(self)

    # TestDtypeTests
    def __hash__(self) -> int:
        """Hash."""
        return hash(str(self))

    # TestDtypeTests
    def __eq__(self, other: Any) -> bool:
        """Test TS equality."""
        if isinstance(other, str):
            return self.name == other
        return isinstance(other, type(self)) and self.ts_class == other.ts_class

    # Required for pickle compat (see GH26067)
    def __setstate__(self, state) -> None:
        """__setstate__."""
        self._ts_class = state["ts_class"]

    # Required for all ExtensionDtype subclasses
    @classmethod
    def construct_array_type(cls):
        """Return the array type associated with this dtype."""
        from tstore.tsdf.extensions.array import TSArray

        return TSArray

    # Recommended for parameterized dtypes
    @classmethod
    def construct_from_string(cls, string: str):
        """
        Construct an TSDtype from a string.

        Example:
        -------
        >>> TSDtype.construct_from_string("TS[pandas]")
        TS['pandas']
        """
        if not isinstance(string, str):
            msg = f"'construct_from_string' expects a string, got {type(string)}"
            raise TypeError(msg)

        msg = f"Cannot construct a '{cls.__name__}' from '{string}'"
        match = cls._match.match(string)

        if match:
            d = match.groupdict()
            try:
                return cls(ts_class=d["ts_class"])
            except (KeyError, TypeError, ValueError) as err:
                raise TypeError(msg) from err
        else:
            raise TypeError(msg)

    @property
    def ts_class(self) -> str:
        """The TS object class."""
        return self._ts_class


register_extension_dtype(TSDtype)
