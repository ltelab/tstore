"""TSDF package."""

from pathlib import Path
from typing import Union

from tstore.backend import Backend
from tstore.tsdf.pandas import TSDFPandas
from tstore.tsdf.tsdf import TSDF


def open_tsdf(base_dir: Union[str, Path], *args, backend: Backend = "pandas", **kwargs):
    """Read a TStore file structure as a TSDF object."""
    tsdf_classes = {
        "pandas": TSDFPandas,
    }

    if backend not in tsdf_classes:
        raise ValueError(f'Backend "{backend}" is not supported.')

    return tsdf_classes[backend].from_tstore(base_dir, *args, **kwargs)


__all__ = [
    "open_tsdf",
    "TSDF",
]
