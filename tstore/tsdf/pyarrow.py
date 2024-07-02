"""TSDF class wrapping a PyArrow dataframe of TSArray objects."""

from typing import TYPE_CHECKING

from tstore.tsdf.tsdf import TSDF, get_ts_columns

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.pyarrow import TSLongPyArrow


class TSDFPyArrow(TSDF):
    """A dataframe class with additional functionality for TSArray data."""

    def to_tstore(self):
        """Write TStore from TSDF object."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir: str) -> "TSDFPyArrow":
        """Read TStore into TSDF object."""
        raise NotImplementedError

    @property
    def _tstore_ts_vars(self) -> dict[str, list[str]]:
        """Return the dictionary of time-series column names."""
        df = self._obj
        ts_cols = get_ts_columns(df)
        ts_objects = {col: df[col].iloc[0] for col in ts_cols}
        return {
            col: [var for var in ts_obj._obj.schema.names if var != ts_obj._tstore_time_var]
            for col, ts_obj in ts_objects.items()
        }

    def to_tslong(self) -> "TSLongPyArrow":
        """Convert the wrapper into a TSLong object."""
        raise NotImplementedError
