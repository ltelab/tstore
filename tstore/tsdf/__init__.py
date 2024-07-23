"""TSDF package."""

from pathlib import Path
from typing import Union

from tstore.backend import Backend
from tstore.tsdf.tsdf import TSDF


def open_tsdf(base_dir: Union[str, Path], *args, backend: Backend = "dask", **kwargs):
    """Read a TStore file structure as a TSDF object."""
    return TSDF.from_tstore(base_dir, *args, backend=backend, **kwargs)


__all__ = [
    "open_tsdf",
    "TSDF",
]
