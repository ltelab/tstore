"""tstore."""

import contextlib
from importlib.metadata import PackageNotFoundError, version

from tstore.tsdf import TSDF, open_tsdf
from tstore.tsdf.ts_class import TS
from tstore.tsdf.ts_dtype import TSDtype
from tstore.tsdf.tsarray import TSArray
from tstore.tslong import TSLong, open_tslong
from tstore.tswide import TSWide, open_tswide

__all__ = [
    "open_tsdf",
    "open_tslong",
    "open_tswide",
    "TS",
    "TSArray",
    "TSDF",
    "TSDtype",
    "TSLong",
    "TSWide",
]

# Get version
with contextlib.suppress(PackageNotFoundError):
    __version__ = version("tstore")
