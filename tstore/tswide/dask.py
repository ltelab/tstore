"""Module defining the TSWideDask wrapper."""

from typing import TYPE_CHECKING

from tstore.tswide.tswide import TSWide

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.dask import TSLongDask


class TSWideDask(TSWide):
    """Wrapper for a long-form Dask timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir: str) -> "TSWideDask":
        """Open a TStore file structure as a TSWideDask wrapper around a Dask long dataframe."""
        raise NotImplementedError

    def to_tslong(self) -> "TSLongDask":
        """Convert the wrapper into a TSLongDask object."""
        raise NotImplementedError
