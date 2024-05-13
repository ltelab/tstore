"""Define possible backends for type hinting."""

from typing import Literal, Union

import dask.dataframe as dd
import pandas as pd
import polars as pl
import pyarrow as pa

Backend = Literal[
    "dask",
    "pandas",
    "polars",
    "pyarrow",
]

DaskDataFrame = dd.DataFrame
PandasDataFrame = pd.DataFrame
PolarsDataFrame = pl.DataFrame
PyArrowDataFrame = pa.Table
DataFrame = Union[DaskDataFrame, PandasDataFrame, PolarsDataFrame, PyArrowDataFrame]
