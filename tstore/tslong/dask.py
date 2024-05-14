"""Module defining the TSLongDask wrapper."""

from tstore.tslong.tslong import TSLong


class TSLongDask(TSLong):
    """Wrapper for a long-form Dask timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir) -> "TSLongDask":
        """Open a TStore file structure as a TSLongDask wrapper around a Dask long dataframe."""
        raise NotImplementedError
