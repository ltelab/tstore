"""Module defining the TSWidePandas wrapper."""

from typing import TYPE_CHECKING

from tstore.tswide.tswide import TSWide

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.pandas import TSLongPandas


class TSWidePandas(TSWide):
    """Wrapper for a long-form Pandas timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir: str) -> "TSWidePandas":
        """Open a TStore file structure as a TSWidePandas wrapper around a Pandas long dataframe."""
        raise NotImplementedError

    def to_tslong(self) -> "TSLongPandas":
        """Convert the wrapper into a TSLongPandas object."""
        raise NotImplementedError
