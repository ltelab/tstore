"""Module defining the TSWidePolars wrapper."""

from tstore.tswide.tswide import TSWide


class TSWidePolars(TSWide):
    """Wrapper for a long-form Polars timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir) -> "TSWidePolars":
        """Open a TStore file structure as a TSWidePolars wrapper around a Polars long dataframe."""
        raise NotImplementedError
