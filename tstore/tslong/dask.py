"""Module defining the TSLongDask wrapper."""

from typing import TYPE_CHECKING

from tstore.tslong.pyarrow import TSLongPyArrow
from tstore.tslong.tslong import TSLong

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tsdf.dask import TSDFDask
    from tstore.tswide.dask import TSWideDask


class TSLongDask(TSLong):
    """Wrapper for a long-form Dask timeseries dataframe."""

    def to_tstore(
        self,
        base_dir,
        partitioning=None,
        tstore_structure="id-var",
        overwrite=True,
    ):
        """Write the wrapped dataframe as a TStore structure."""
        pandas_tslong = self.change_backend(new_backend="pandas")
        pandas_tslong.to_tstore(
            base_dir=base_dir,
            partitioning=partitioning,
            tstore_structure=tstore_structure,
            overwrite=overwrite,
        )

    @staticmethod
    def from_tstore(
        base_dir,
        ts_variables=None,
        start_time=None,
        end_time=None,
        tstore_ids=None,
        columns=None,
        filesystem=None,
        use_threads=True,
    ) -> "TSLongDask":
        """Open a TStore file structure as a TSLongDask wrapper around a Pandas long dataframe."""
        # Read exploiting pyarrow
        tslong_pyarrow = TSLongPyArrow.from_tstore(
            base_dir,
            ts_variables=ts_variables,
            start_time=start_time,
            end_time=end_time,
            tstore_ids=tstore_ids,
            columns=columns,
            filesystem=filesystem,
            use_threads=use_threads,
        )

        # Conversion to pandas
        return tslong_pyarrow.change_backend(new_backend="dask")

    def to_tsdf(self) -> "TSDFDask":
        """Convert the wrapper into a TSDFDask object."""
        raise NotImplementedError

    def to_tswide(self) -> "TSWideDask":
        """Convert the wrapper into a TSWideDask object."""
        raise NotImplementedError
