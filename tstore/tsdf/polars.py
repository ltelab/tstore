"""TSDF class wrapping a Polars dataframe of TSArray objects."""

from typing import TYPE_CHECKING

from tstore.tsdf.tsdf import TSDF

if TYPE_CHECKING:
    # To avoid circular imports
    pass


class TSDFPolars(TSDF):
    """A dataframe class with additional functionality for TSArray data."""

    def to_tstore(self):
        """Write TStore from TSDF object."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir: str) -> "TSDFPolars":
        """Read TStore into TSDF object."""
        raise NotImplementedError
