"""Module defining the TSLongDask wrapper."""

from typing import TYPE_CHECKING

import geopandas as gpd
import pandas as pd

from tstore.tsdf.ts_class import TS
from tstore.tsdf.tsarray import TSArray
from tstore.tslong.pyarrow import TSLongPyArrow
from tstore.tslong.tslong import TSLong

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tsdf.tsdf import TSDF
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

    def to_tsdf(self) -> "TSDF":
        """Convert the wrapper into a TSDF object with Dask TS objects."""
        from tstore.tsdf.tsdf import TSDF

        tstore_ids = self._get_tstore_ids()
        ts_arrays = self._get_ts_arrays()
        pd_series = {ts_variable: pd.Series(ts_array, index=tstore_ids) for ts_variable, ts_array in ts_arrays.items()}
        static_values = self._get_static_values()
        data = {**pd_series, **static_values, self._tstore_id_var: tstore_ids}

        df = pd.DataFrame(data)

        if self._tstore_geometry is not None:
            df = df.merge(self._tstore_geometry, on=self._tstore_id_var, how="left")
            df = gpd.GeoDataFrame(df, geometry=self._tstore_geometry.geometry.name)

        return TSDF(
            df,
            id_var=self._tstore_id_var,
        )

    def _get_tstore_ids(self) -> list:
        """Retrieve the list of tstore ids."""
        return sorted(self._obj[self._tstore_id_var].unique())

    def _get_ts_arrays(self) -> dict[str, TSArray]:
        """Create a dictionary of TSArrays."""
        return {ts_variable: self._get_ts_array(variables) for ts_variable, variables in self._tstore_ts_vars.items()}

    def _get_ts_array(self, variables: list[str]) -> TSArray:
        """Create a TSArray for a set of variables."""
        tstore_ids = self._get_tstore_ids()
        ts_list = [self._get_ts(tstore_id, variables) for tstore_id in tstore_ids]
        return TSArray(ts_list)

    def _get_ts(self, tstore_id: str, variables: list[str]) -> TS:
        """Create a TS object for a given tstore_id and a set of variables."""
        df = self._obj
        df = df[df[self._tstore_id_var] == tstore_id]
        df = df[variables]
        return TS(df)

    def _get_static_values(self) -> dict[str, list]:
        """Retrieve the static values."""
        df = self._obj
        tstore_ids = self._get_tstore_ids()
        static_values = {}

        for static_var in self._tstore_static_vars:
            var_static_values = []
            for tstore_id in tstore_ids:
                values = df[df[self._tstore_id_var] == tstore_id][static_var].unique().compute()

                if len(values) > 1:
                    raise ValueError(
                        f"Static variables should be unique per tstore_id. Found {list(values)} for {static_var}.",
                    )

                var_static_values.append(values[0])

            static_values[static_var] = var_static_values

        return static_values

    def to_tswide(self) -> "TSWideDask":
        """Convert the wrapper into a TSWideDask object."""
        from tstore.tswide.dask import TSWideDask

        df = self._obj
        df = df.reset_index()
        df[self._tstore_id_var] = df[self._tstore_id_var].astype("category").compute()
        df = df.pivot_table(
            index=self._tstore_time_var,
            columns=self._tstore_id_var,
            values=df.columns.difference([self._tstore_id_var]),
            aggfunc="first",
        )

        return TSWideDask(
            df,
            id_var=self._tstore_id_var,
            time_var=self._tstore_time_var,
            ts_vars=self._tstore_ts_vars,
            static_vars=self._tstore_static_vars,
            geometry=self._tstore_geometry,
        )
