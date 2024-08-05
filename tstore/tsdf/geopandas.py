"""TSDF class wrapping a GeoPandas dataframe of TSArray objects."""

from tstore.tsdf.ts_dtype import TSDtype
from tstore.tsdf.tsdf import TSDF


class TSDFGeoPandas(TSDF):
    """Wrapper for a GeoPandas DataFrame of TSArray objects."""

    @property
    def _tstore_geometry(self):
        """Return the geometry column."""
        return self._obj.geometry

    @property
    def _tstore_static_vars(self) -> list[str]:
        """Return the list of static column names."""
        df = self._obj

        return [
            col
            for col in df.columns
            if col != self._tstore_id_var
            and col != self._tstore_geometry.geometry.name
            and not isinstance(df[col].dtype, TSDtype)
        ]
