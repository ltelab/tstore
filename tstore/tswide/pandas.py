"""Module defining the TSWidePandas wrapper."""

from tstore.tswide.tswide import TSWide


class TSWidePandas(TSWide):
    """Wrapper for a long-form Pandas timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir) -> "TSWidePandas":
        """Open a TStore file structure as a TSWidePandas wrapper around a Pandas long dataframe."""
        raise NotImplementedError
