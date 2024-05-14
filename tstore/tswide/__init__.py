"""TSWide package."""

from pathlib import Path
from typing import Union

from tstore.backend import Backend
from tstore.tswide.dask import TSWideDask
from tstore.tswide.pandas import TSWidePandas
from tstore.tswide.polars import TSWidePolars
from tstore.tswide.pyarrow import TSWidePyArrow
from tstore.tswide.tswide import TSWide


def open_tswide(base_dir: Union[str, Path], *args, backend: Backend = "pandas", **kwargs):
    """Read a TStore file structure as a TSWide object."""
    ts_wide_classes = {
        "dask": TSWideDask,
        "pandas": TSWidePandas,
        "polars": TSWidePolars,
        "pyarrow": TSWidePyArrow,
    }

    if backend not in ts_wide_classes:
        raise ValueError(f'Backend "{backend}" is not supported.')

    return ts_wide_classes[backend].from_tstore(base_dir, *args, **kwargs)


__all__ = [
    "open_tswide",
    "TSWide",
]
