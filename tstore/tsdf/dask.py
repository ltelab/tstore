"""TSDF class wrapping a Dask dataframe of TSArray objects."""

from typing import TYPE_CHECKING

from tstore.tsdf.tsdf import TSDF

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.dask import TSLongDask


class TSDFDask(TSDF):
    """A dataframe class with additional functionality for TSArray data."""

    def to_tstore(self):
        """Write TStore from TSDF object."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir: str) -> "TSDFDask":
        """Read TStore into TSDF object."""
        raise NotImplementedError

    def to_tslong(self) -> "TSLongDask":
        """Convert the wrapper into a TSLong object."""
        raise NotImplementedError
