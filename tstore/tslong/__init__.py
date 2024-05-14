"""TSLong package."""

from pathlib import Path
from typing import Union

from tstore.backend import Backend
from tstore.tslong.dask import TSLongDask
from tstore.tslong.pandas import TSLongPandas
from tstore.tslong.polars import TSLongPolars
from tstore.tslong.pyarrow import TSLongPyArrow
from tstore.tslong.tslong import TSLong


def open_tslong(base_dir: Union[str, Path], *args, backend: Backend = "pandas", **kwargs):
    """Read a TStore file structure as a TSLong object."""
    ts_long_classes = {
        "dask": TSLongDask,
        "pandas": TSLongPandas,
        "polars": TSLongPolars,
        "pyarrow": TSLongPyArrow,
    }

    if backend not in ts_long_classes:
        raise ValueError(f'Backend "{backend}" is not supported.')

    return ts_long_classes[backend].from_tstore(base_dir, *args, **kwargs)


__all__ = [
    "open_tslong",
    "TSLong",
]
