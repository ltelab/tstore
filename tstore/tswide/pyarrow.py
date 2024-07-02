"""Module defining the TSWidePyArrow wrapper."""

from typing import TYPE_CHECKING

from tstore.tswide.tswide import TSWide

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tslong.pyarrow import TSLongPyArrow


class TSWidePyArrow(TSWide):
    """Wrapper for a long-form PyArrow timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir: str) -> "TSWidePyArrow":
        """Open a TStore file structure as a TSWidePyArrow wrapper around a PyArrow long dataframe."""
        raise NotImplementedError

    def to_tslong(self) -> "TSLongPyArrow":
        """Convert the wrapper into a TSLongPyArrow object."""
        raise NotImplementedError
