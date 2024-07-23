"""TSDF class wrapping a Pandas dataframe of TSArray objects."""

from tstore.tsdf.tsdf import TSDF


class TSDFPandas(TSDF):
    """Wrapper for a Pandas DataFrame of TSArray objects."""
