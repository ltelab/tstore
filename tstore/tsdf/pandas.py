"""TSDF class wrapping a Pandas dataframe of TSArray objects."""

from typing import TYPE_CHECKING

from tstore.tsdf.tsdf import TSDF

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.pandas import TSLongPandas


class TSDFPandas(TSDF):
    """A dataframe class with additional functionality for TSArray data."""

    def to_tstore(self):
        """Write TStore from TSDF object."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir: str) -> "TSDFPandas":
        """Read TStore into TSDF object."""
        raise NotImplementedError

    def to_tslong(self) -> "TSLongPandas":
        """Convert the wrapper into a TSLong object."""
        raise NotImplementedError
