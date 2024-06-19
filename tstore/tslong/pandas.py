"""Module defining the TSLongPandas wrapper."""

from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa

from tstore.archive.io import (
    check_tstore_directory,
    check_tstore_structure,
    define_attributes_filepath,
    define_tsarray_filepath,
)
from tstore.archive.metadata.writers import write_tstore_metadata
from tstore.archive.partitions import add_partitioning_columns, check_partitioning
from tstore.archive.ts.writers.pyarrow import write_partitioned_dataset
from tstore.tsdf.ts_class import TS
from tstore.tsdf.tsarray import TSArray
from tstore.tslong.pyarrow import TSLongPyArrow
from tstore.tslong.tslong import TSLong

if TYPE_CHECKING:
    # To avoid circular imports
    from tstore.tsdf.pandas import TSDFPandas
    from tstore.tswide.pandas import TSWidePandas


class TSLongPandas(TSLong):
    """Wrapper for a long-form Pandas timeseries dataframe."""

    def to_tstore(
        self,
        base_dir,
        partitioning=None,
        tstore_structure="id-var",
        overwrite=True,
    ):
        """Write the wrapped dataframe as a TStore structure."""
        # If index time, remove
        if self._tstore_time_var not in self._obj.columns:
            self._obj = self._obj.reset_index(names=self._tstore_time_var)

        # Set identifier as pyarrow large_string !
        # - Very important to join attrs at read time !
        self._obj[self._tstore_id_var] = self._obj[self._tstore_id_var].astype("large_string[pyarrow]")

        # Check inputs
        tstore_structure = check_tstore_structure(tstore_structure)
        base_dir = check_tstore_directory(base_dir, overwrite=overwrite)

        # Check ts_variables
        ts_variables = self._tstore_ts_vars
        if isinstance(ts_variables, list):
            ts_variables = {column: None for column in ts_variables}

        # Identify all ts columns
        # ts_columns = set(df.columns) - set([self._tstore_time_var]) - set(static_variables)

        # Check which columns remains (not specified at class init)
        # TODO

        # Check partitioning
        partitioning = check_partitioning(partitioning, ts_variables=list(ts_variables))

        # Identify static dataframe (attributes)
        attr_cols = [self._tstore_id_var]
        if self._tstore_static_vars is not None:
            attr_cols += self._tstore_static_vars
        # - TODO: add flag to check if are actual duplicates !
        df_attrs = self._obj[attr_cols]
        df_attrs = df_attrs.drop_duplicates(subset=self._tstore_id_var)
        df_attrs = df_attrs.reset_index(drop=True)

        # Write static attributes
        # --> Need to test that save id_var as large_string !
        attrs_fpath = define_attributes_filepath(base_dir)
        df_attrs.to_parquet(
            attrs_fpath,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        # Write tstore metadata
        write_tstore_metadata(
            base_dir=base_dir,
            id_var=self._tstore_id_var,
            time_var=self._tstore_time_var,
            ts_variables=list(ts_variables),
            tstore_structure=tstore_structure,
            partitioning=partitioning,
        )

        # Write to disk per identifier
        for tstore_id, df_group in self._obj.groupby(self._tstore_id_var):
            for ts_variable, columns in ts_variables.items():
                # Retrieve columns of the TS object
                if columns is None:
                    columns = [ts_variable]

                # Retrieve TS object
                df_ts = df_group[[*columns, self._tstore_time_var]].copy()
                df_ts = df_ts.reset_index(drop=True)

                # Check time is sorted ?
                # TODO

                # Add partitioning columns
                partitioning_str = partitioning[ts_variable]
                df_ts, partitions = add_partitioning_columns(
                    df_ts,
                    partitioning_str=partitioning_str,
                    time_var=self._tstore_time_var,
                    backend="pandas",
                )

                # Define filepath of partitioned TS
                ts_fpath = define_tsarray_filepath(
                    base_dir=base_dir,
                    tstore_id=tstore_id,
                    ts_variable=ts_variable,
                    tstore_structure=tstore_structure,
                )

                # -----------------------------------------------
                # Maybe wrap df_ts into TS and then use write_partitioned_dataset within TS.to_parquet

                # Retrieve pyarrow Table
                table = pa.Table.from_pandas(df_ts)
                write_partitioned_dataset(
                    base_dir=ts_fpath,
                    table=table,
                    partitioning=partitions,
                    # row_group_size="400MB",
                    # max_file_size="2GB",
                    # compression="snappy",
                    # compression_level=None,
                    # # Computing options
                    # max_open_files=0,
                    # use_threads=True,
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
    ) -> "TSLongPandas":
        """Open a TStore file structure as a TSLongPandas wrapper around a Pandas long dataframe."""
        # Read exploiting pyarrow
        # - We use pyarrow to avoid pandas copies at concatenation and join operations !
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
        return tslong_pyarrow.change_backend(new_backend="pandas")

    def to_tsdf(self) -> "TSDFPandas":
        """Convert the wrapper into a TSDF object."""
        from tstore.tsdf.pandas import TSDFPandas

        tstore_ids = self._get_tstore_ids()
        ts_arrays = self._get_ts_arrays()
        pd_series = {ts_variable: pd.Series(ts_array, index=tstore_ids) for ts_variable, ts_array in ts_arrays.items()}
        static_values = self._get_static_values()
        data = {**pd_series, **static_values, self._tstore_id_var: tstore_ids}

        df = pd.DataFrame(data)
        return TSDFPandas(
            df,
            id_var=self._tstore_id_var,
            time_var=self._tstore_time_var,
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
        df = df[[self._tstore_time_var, *variables]]
        return TS(df)

    def _get_static_values(self) -> dict[str, list]:
        """Retrieve the static values."""
        df = self._obj
        tstore_ids = self._get_tstore_ids()
        static_values = {}

        for static_var in self._tstore_static_vars:
            var_static_values = []
            for tstore_id in tstore_ids:
                values = df[df[self._tstore_id_var] == tstore_id][static_var].unique()

                if len(values) > 1:
                    raise ValueError(
                        f"Static variables should be unique per tstore_id. Found {list(values)} for {static_var}.",
                    )

                var_static_values.append(values[0])

            static_values[static_var] = var_static_values

        return static_values

    def to_tswide(self) -> "TSWidePandas":
        """Convert the wrapper into a TSWide object."""
        raise NotImplementedError
