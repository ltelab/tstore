"""Module defining the TSLongDask wrapper."""

from typing import TYPE_CHECKING

from tstore.tslong.tslong import TSLong

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tsdf.dask import TSDFDask
    from tstore.tswide.dask import TSWideDask


class TSLongDask(TSLong):
    """Wrapper for a long-form Dask timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir) -> "TSLongDask":
        """Open a TStore file structure as a TSLongDask wrapper around a Dask long dataframe."""
        raise NotImplementedError

    def to_tsdf(self) -> "TSDFDask":
        """Convert the wrapper into a TSDFDask object."""
        raise NotImplementedError

    def to_tswide(self) -> "TSWideDask":
        """Convert the wrapper into a TSWideDask object."""
        raise NotImplementedError
