"""tstore."""

import contextlib
from importlib.metadata import PackageNotFoundError, version

from tstore.tsdf import TSDF
from tstore.tsdf.extensions.array import TSArray
from tstore.tsdf.extensions.ts_dtype import TSDtype
from tstore.tsdf.reader import open_tsdf
from tstore.tsdf.ts_class import TS
from tstore.tslong.pandas import open_tslong

__all__ = [
    "open_tsdf",
    "TSArray",
    "TSDtype",
    "TS",
    "TSDF",
]

# Get version
with contextlib.suppress(PackageNotFoundError):
    __version__ = version("tstore")
