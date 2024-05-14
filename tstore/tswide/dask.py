"""Module defining the TSWideDask wrapper."""

from tstore.tswide.tswide import TSWide


class TSWideDask(TSWide):
    """Wrapper for a long-form Dask timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir) -> "TSWideDask":
        """Open a TStore file structure as a TSWideDask wrapper around a Dask long dataframe."""
        raise NotImplementedError
