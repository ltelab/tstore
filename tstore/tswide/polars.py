"""Module defining the TSWidePolars wrapper."""

from typing import TYPE_CHECKING

from tstore.tswide.tswide import TSWide

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.polars import TSLongPolars


class TSWidePolars(TSWide):
    """Wrapper for a long-form Polars timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir: str) -> "TSWidePolars":
        """Open a TStore file structure as a TSWidePolars wrapper around a Polars long dataframe."""
        raise NotImplementedError

    def to_tslong(self) -> "TSLongPolars":
        """Convert the wrapper into a TSLongPolars object."""
        raise NotImplementedError
