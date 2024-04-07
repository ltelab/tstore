#!/usr/bin/env python3
"""
Created on Sun Jun 11 22:47:54 2023.

@author: ghiggi
"""

import dask.dataframe as dd
import numpy as np
import pandas as pd
from pandas.api.extensions import ExtensionArray

from tstore.tsdf.extensions.ts_dtype import TSDtype
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


####--------------------------------------------------------------------------.
def get_tabular_object_type(obj):
    """Get inner class of the TS object."""
    if isinstance(obj, dd.DataFrame):
        return "dask.DataFrame"
    if isinstance(obj, pd.DataFrame):
        return "dask.Series"
    if isinstance(obj, pd.DataFrame):
        return "pandas.DataFrame"
    if isinstance(obj, pd.Series):
        return "pandas.Series"
    return type(obj).__name__


class TSArray(ExtensionArray):
    """An ExtensionArray for TS objects, holding the array-based implementations."""

    _dtype = TSDtype()

    def __init__(self, data, copy: bool = False):
        self._data = np.array(data, copy=copy)

    @property
    def _class(self):
        """Define inner TS class."""
        # Infer TS class from TS objects
        # TODO: Now first. In future loop and check unique ...

        #### BUG HERE
        # If no matching index, when joining during open_tsdf, we get
        #  array([nan, nan, nan, nan], dtype=object) here
        # --> Need to create empty object !
        # TS[empty]
        ts_object = self._data[0]
        tabular_object = getattr(ts_object, "data", pd.Series())
        ts_class = get_tabular_object_type(tabular_object)
        return ts_class

    def __str__(self):
        """String representation."""
        # TODO print
        return str(self._data)

    def __repr__(self):
        """Repr representation."""
        n = len(self._data)
        return f"TSArray composed of {n} TS objects."

    # Required for all ExtensionArray subclasses
    def __getitem__(self, index: int):
        """Select a subset of self."""
        if isinstance(index, int):
            return self._data[index]
        # Check index for TestGetitemTests
        index = pd.core.indexers.check_array_indexer(self, index)
        return type(self)(self._data[index])

    # TestSetitemTests
    def __setitem__(self, index: int, value: TS) -> None:
        """Set one or more values in-place."""
        # Check index for TestSetitemTests
        index = pd.core.indexers.check_array_indexer(self, index)

        # Upcast to value's type (if needed) for TestMethodsTests
        if self._data.dtype < type(value):
            self._data = self._data.astype(type(value))

        # TODO: Validate value for TestSetitemTests
        # value = self._validate_setitem_value(value)

        self._data[index] = value

    # Required for all ExtensionArray subclasses
    def __len__(self) -> int:
        """Length of this array."""
        return len(self._data)

    # Required for all ExtensionArray subclasses
    @pd.core.ops.unpack_zerodim_and_defer("__eq__")
    def __eq__(self, other):
        """Equality behaviour."""
        # TODO: how to compare list of TS objects
        return False

    # Required for all ExtensionArray subclasses
    @classmethod
    def _from_sequence(cls, data, dtype=None, copy: bool = False):
        """Construct a new TSArray from a sequence of TS."""
        if dtype is None:
            dtype = TSDtype()

        if not isinstance(dtype, TSDtype):
            msg = f"'{cls.__name__}' only supports 'TSDtype' dtype"
            raise ValueError(msg)
        return cls(data, copy=copy)

    # Required for all ExtensionArray subclasses
    @classmethod
    def _concat_same_type(cls, to_concat):
        """Concatenate multiple TSArrays."""
        # Ensure same TS class
        counts = pd.value_counts([array.dtype.ts_class for array in to_concat])
        if counts.size > 1:
            raise ValueError("The TS objects must all be of the same type.")

        return cls(np.concatenate(to_concat))

    # Required for all ExtensionArray subclasses
    @property
    def dtype(self):
        """An instance of TSDtype."""
        return TSDtype(self._class)

    # Required for all ExtensionArray subclasses
    @property
    def nbytes(self) -> int:
        """The number of bytes needed to store this object in memory."""
        return self._data.nbytes

    @property
    def ts_class(self):
        """TS inner class."""
        return self.dtype.ts_class

    # Required for all ExtensionArray subclasses
    def isna(self):
        """A 1-D array indicating if the TS is missing."""
        return pd.isnull(self._data)

    # Required for all ExtensionArray subclasses
    def copy(self):
        """Return a copy of the array."""
        copied = self._data.copy()
        return type(self)(copied)

    # Required for all ExtensionArray subclasses
    def take(self, indices, allow_fill=False, fill_value=None):
        """Take elements from an array."""
        if allow_fill and fill_value is None:
            fill_value = self.dtype.na_value

        result = pd.core.algorithms.take(
            self._data,
            indices,
            allow_fill=allow_fill,
            fill_value=fill_value,
        )
        return self._from_sequence(result)
