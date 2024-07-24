"""TSDF class wrapping a GeoPandas dataframe of TSArray objects."""

from tstore.tsdf.tsdf import TSDF


class TSDFGeoPandas(TSDF):
    """Wrapper for a GeoPandas DataFrame of TSArray objects."""
