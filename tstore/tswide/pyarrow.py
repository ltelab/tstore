"""Module defining the TSWidePyArrow wrapper."""

from tstore.tswide.tswide import TSWide


class TSWidePyArrow(TSWide):
    """Wrapper for a long-form PyArrow timeseries dataframe."""

    def to_tstore(self):
        """Write the wrapped dataframe as a TStore structure."""
        raise NotImplementedError

    @staticmethod
    def from_tstore(base_dir) -> "TSWidePyArrow":
        """Open a TStore file structure as a TSWidePyArrow wrapper around a PyArrow long dataframe."""
        raise NotImplementedError
