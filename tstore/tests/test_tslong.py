"""Test the tslong subpackage."""

import os
from pathlib import Path
from typing import Literal

import dask.dataframe as dd
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
import yaml

import tstore
from tstore.backend import Backend
from tstore.tsdf.pandas import TSDFPandas
from tstore.tslong.dask import TSLongDask
from tstore.tslong.pandas import TSLongPandas
from tstore.tslong.polars import TSLongPolars
from tstore.tslong.pyarrow import TSLongPyArrow
from tstore.tswide.dask import TSWideDask
from tstore.tswide.pandas import TSWidePandas
from tstore.tswide.polars import TSWidePolars
from tstore.tswide.pyarrow import TSWidePyArrow

# Imported fixtures from conftest.py:
# - dask_long_dataframe
# - dask_long_geo_dataframe
# - dask_tslong
# - pandas_long_dataframe
# - pandas_long_geo_dataframe
# - pandas_tslong
# - polars_long_dataframe
# - polars_long_geo_dataframe
# - polars_tslong
# - pyarrow_long_dataframe
# - pyarrow_long_geo_dataframe
# - pyarrow_tslong


WithGeo = Literal["without_geo", "with_geo"]

tslong_classes = {
    "dask": TSLongDask,
    "pandas": TSLongPandas,
    "polars": TSLongPolars,
    "pyarrow": TSLongPyArrow,
}

tswide_classes = {
    "dask": TSWideDask,
    "pandas": TSWidePandas,
    "polars": TSWidePolars,
    "pyarrow": TSWidePyArrow,
}


# Functions ####################################################################


def store_tslong(tslong: tstore.TSLong, dirpath: str) -> None:
    tstore_structure = "id-var"
    overwrite = True

    # Same partitioning for all TS
    partitioning = "year/month"
    # Partitioning specific to each TS
    partitioning = {"ts_var1": "year/month", "ts_var2": "year/month"}

    tslong.to_tstore(
        # TSTORE options
        dirpath,
        # TSTORE options
        partitioning=partitioning,
        tstore_structure=tstore_structure,
        overwrite=overwrite,
    )


# Tests ########################################################################


@pytest.mark.parametrize("backend", ["dask", "pandas", "polars", "pyarrow"])
def test_creation(
    backend: Backend,
    request,
) -> None:
    """Test the creation of a TSLong object."""
    dataframe_fixture_name = f"{backend}_long_dataframe"
    df = request.getfixturevalue(dataframe_fixture_name)
    tslong = tstore.TSLong.wrap(
        df,
        id_var="tstore_id",
        time_var="time",
        ts_vars={"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]},
        static_vars=["static_var1", "static_var2"],
    )

    assert isinstance(tslong, tslong_classes[backend])
    assert tslong.current_backend == backend

    if isinstance(tslong, TSLongDask):
        assert tslong._obj.compute().shape == df.compute().shape
    else:
        assert tslong.shape == df.shape

    assert tslong._tstore_id_var == "tstore_id"
    assert tslong._tstore_time_var == "time"
    assert tslong._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
    assert tslong._tstore_static_vars == ["static_var1", "static_var2"]

    if isinstance(tslong, (TSLongDask, TSLongPandas)):
        assert tslong["tstore_id"].dtype == "large_string[pyarrow]"
    elif isinstance(tslong, TSLongPolars):
        assert tslong["tstore_id"].dtype == pl.String
    elif isinstance(tslong, TSLongPyArrow):
        assert tslong["tstore_id"].type == pa.large_string()


@pytest.mark.parametrize("backend", ["dask", "pandas", "polars", "pyarrow"])
class TestCreationArgs:
    """Test the creation of a TSLong object with various arguments."""

    @pytest.fixture()
    def base_kwargs(self, backend: Backend, request) -> dict:
        """Return a template of keyword arguments for the TSLong wrapper."""
        dataframe_fixture_name = f"{backend}_long_dataframe"
        df = request.getfixturevalue(dataframe_fixture_name)

        return {
            "df": df,
            "id_var": "tstore_id",
            "time_var": "time",
            "ts_vars": {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]},
            "static_vars": ["static_var1", "static_var2"],
        }

    def test_ts_vars_none(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with ts_vars=None."""
        kwargs = base_kwargs.copy()
        kwargs["ts_vars"] = None
        tslong = tstore.TSLong.wrap(**kwargs)
        assert tslong._tstore_ts_vars == {"ts_variable": ["var1", "var2", "var3", "var4"]}

    def test_ts_vars_list(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with ts_vars as a list."""
        kwargs = base_kwargs.copy()
        kwargs["ts_vars"] = ["var1", "var2", "var3", "var4"]
        tslong = tstore.TSLong.wrap(**kwargs)
        assert tslong._tstore_ts_vars == {"var1": ["var1"], "var2": ["var2"], "var3": ["var3"], "var4": ["var4"]}

    def test_ts_vars_incomplete_list(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with ts_vars as a list not covering all columns."""
        kwargs = base_kwargs.copy()
        kwargs["ts_vars"] = ["var1", "var2"]
        with pytest.raises(ValueError):
            tstore.TSLong.wrap(**kwargs)

    def test_ts_vars_list_with_unknown_columns(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with ts_vars as a list containing unknown columns."""
        kwargs = base_kwargs.copy()
        kwargs["ts_vars"] = ["var1", "var2", "var3", "var4", "var5"]
        with pytest.raises(ValueError):
            tstore.TSLong.wrap(**kwargs)

    def test_ts_vars_list_with_id_var(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with ts_vars as a list containing the id_var."""
        kwargs = base_kwargs.copy()
        kwargs["ts_vars"] = ["var1", "var2", "var3", "var4", "tstore_id"]
        with pytest.raises(ValueError):
            tstore.TSLong.wrap(**kwargs)

    def test_ts_vars_list_with_time_var(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with ts_vars as a list containing the time_var."""
        kwargs = base_kwargs.copy()
        kwargs["ts_vars"] = ["var1", "var2", "var3", "var4", "time"]
        with pytest.raises(ValueError):
            tstore.TSLong.wrap(**kwargs)

    def test_ts_vars_dict(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with ts_vars as a dict."""
        kwargs = base_kwargs.copy()
        kwargs["ts_vars"] = {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
        tslong = tstore.TSLong.wrap(**kwargs)
        assert tslong._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}

    def test_invalid_time_var(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with an invalid time_var."""
        kwargs = base_kwargs.copy()
        kwargs["time_var"] = "invalid_time"
        with pytest.raises(ValueError):
            tstore.TSLong.wrap(**kwargs)

    def test_invalid_id_var(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with an invalid id_var."""
        kwargs = base_kwargs.copy()
        kwargs["id_var"] = "invalid_id"
        with pytest.raises(ValueError):
            tstore.TSLong.wrap(**kwargs)

    def test_invalid_static_vars(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with invalid static_vars."""
        kwargs = base_kwargs.copy()
        kwargs["static_vars"] = ["invalid_static"]
        with pytest.raises(ValueError):
            tstore.TSLong.wrap(**kwargs)

    def test_geometry_var(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with a geometry_var."""
        kwargs = base_kwargs.copy()
        kwargs["geometry_var"] = "static_var1"
        tslong = tstore.TSLong.wrap(**kwargs)
        assert tslong._tstore_geometry_var == "static_var1"
        assert tslong._tstore_static_vars == ["static_var1", "static_var2"]

    def test_invalid_geometry_var(
        self,
        base_kwargs: dict,
    ) -> None:
        """Test the creation of a TSLong object with an invalid geometry_var."""
        kwargs = base_kwargs.copy()
        kwargs["geometry_var"] = "invalid_geometry"
        with pytest.raises(ValueError):
            tstore.TSLong.wrap(**kwargs)


@pytest.mark.parametrize("backend", ["dask", "pandas", "polars", "pyarrow"])
@pytest.mark.parametrize("with_geo", ["without_geo", "with_geo"])
def test_store(
    tmp_path: Path,
    backend: Backend,
    with_geo: WithGeo,
    request,
) -> None:
    """Test the to_tstore function."""
    tslong_fixture_name = f"{backend}_tslong" if with_geo == "without_geo" else f"{backend}_geo_tslong"
    tslong = request.getfixturevalue(tslong_fixture_name)
    dirpath = tmp_path / "test_tstore"
    store_tslong(tslong, str(dirpath))

    # Check that the tstore is created
    assert dirpath.is_dir()

    # Check directory content
    assert sorted(os.listdir(dirpath)) == ["1", "2", "3", "4", "_attributes.parquet", "tstore_metadata.yaml"]
    for ts_var in ["ts_var1", "ts_var2"]:
        assert os.listdir(dirpath / "1" / ts_var / "year=2000" / "month=1") == ["part-0.parquet"]

    # Check metadata
    with open(dirpath / "tstore_metadata.yaml") as file:
        metadata = yaml.safe_load(file)

    expected_metadata = {
        "id_var": "tstore_id",
        "ts_variables": ["ts_var1", "ts_var2"],
        "geometry_var": None,
        "partitioning": {"ts_var1": "year/month", "ts_var2": "year/month"},
        "tstore_structure": "id-var",
    }

    if with_geo == "with_geo":
        expected_metadata["geometry_var"] = "geometry"

    assert metadata == expected_metadata


@pytest.mark.parametrize("with_geo", ["without_geo", "with_geo"])
class TestLoad:
    """Test the from_tstore function."""

    def common_checks(self, tslong: tstore.TSLong, with_geo: WithGeo) -> None:
        if isinstance(tslong, TSLongDask):
            assert tslong._obj.compute().shape[0] == 192
        else:
            assert tslong.shape[0] == 192
        assert tslong._tstore_id_var == "tstore_id"
        assert tslong._tstore_time_var == "time"
        assert tslong._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
        if with_geo == "with_geo":
            assert tslong._tstore_static_vars == ["static_var1", "static_var2", "geometry"]
        else:
            assert tslong._tstore_static_vars == ["static_var1", "static_var2"]

    def test_dask(
        self,
        with_geo: WithGeo,
        request,
    ) -> None:
        """Test loading as a Dask DataFrame."""
        tstore_path_fixture_name = "tstore_path" if with_geo == "without_geo" else "geo_tstore_path"
        tstore_path = request.getfixturevalue(tstore_path_fixture_name)
        tslong = tstore.open_tslong(tstore_path, backend="dask", ts_variables=["ts_var1", "ts_var2"])
        assert type(tslong) is TSLongDask
        assert type(tslong._obj) is dd.DataFrame
        self.common_checks(tslong, with_geo)

    def test_pandas(
        self,
        with_geo: WithGeo,
        request,
    ) -> None:
        """Test loading as a Pandas DataFrame."""
        tstore_path_fixture_name = "tstore_path" if with_geo == "without_geo" else "geo_tstore_path"
        tstore_path = request.getfixturevalue(tstore_path_fixture_name)
        tslong = tstore.open_tslong(tstore_path, backend="pandas", ts_variables=["ts_var1", "ts_var2"])
        assert type(tslong) is TSLongPandas
        assert type(tslong._obj) is pd.DataFrame
        self.common_checks(tslong, with_geo)
        # TODO: time column is counted
        # TODO: line order is not preserved
        # TODO: column order is not preserved

    def test_polars(
        self,
        with_geo: WithGeo,
        request,
    ) -> None:
        """Test loading as a Polars DataFrame."""
        tstore_path_fixture_name = "tstore_path" if with_geo == "without_geo" else "geo_tstore_path"
        tstore_path = request.getfixturevalue(tstore_path_fixture_name)
        tslong = tstore.open_tslong(tstore_path, backend="polars", ts_variables=["ts_var1", "ts_var2"])
        assert type(tslong) is TSLongPolars
        assert type(tslong._obj) is pl.DataFrame
        self.common_checks(tslong, with_geo)
        # TODO: time column is counted
        # TODO: line order is not preserved
        # TODO: column order is not preserved

    def test_pyarrow(
        self,
        with_geo: WithGeo,
        request,
    ) -> None:
        """Test loading as a PyArrow Table."""
        tstore_path_fixture_name = "tstore_path" if with_geo == "without_geo" else "geo_tstore_path"
        tstore_path = request.getfixturevalue(tstore_path_fixture_name)
        tslong = tstore.open_tslong(tstore_path, backend="polars", ts_variables=["ts_var1", "ts_var2"])
        assert type(tslong) is TSLongPolars
        assert type(tslong._obj) is pl.DataFrame
        self.common_checks(tslong, with_geo)


@pytest.mark.parametrize("backend_to", ["dask", "pandas", "polars", "pyarrow"])
@pytest.mark.parametrize("backend_from", ["dask", "pandas", "polars", "pyarrow"])
def test_change_backend(
    backend_from: Backend,
    backend_to: Backend,
    request,
) -> None:
    """Test the change_backend method."""
    tslong_fixture_name = f"{backend_from}_tslong"
    tslong = request.getfixturevalue(tslong_fixture_name)
    tslong_new = tslong.change_backend(new_backend=backend_to)
    assert isinstance(tslong_new, tslong_classes[backend_to])


@pytest.mark.parametrize("backend", ["dask", "pandas", "polars", "pyarrow"])
def test_to_tsdf(
    backend: str,
    request,
) -> None:
    """Test the to_tsdf function."""
    tslong_fixture_name = f"{backend}_tslong"
    tslong = request.getfixturevalue(tslong_fixture_name)
    tsdf = tslong.to_tsdf()

    assert isinstance(tsdf, TSDFPandas)
    assert tsdf.get_ts_backend("ts_var1") == backend
    assert tsdf.get_ts_backend("ts_var2") == backend
    assert tsdf._tstore_id_var == "tstore_id"
    assert tsdf._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
    assert tsdf._tstore_static_vars == ["static_var1", "static_var2"]
    assert isinstance(tsdf["ts_var1"], pd.Series)
    assert isinstance(tsdf["ts_var2"], pd.Series)
    np.testing.assert_array_equal(tsdf["tstore_id"], ["1", "2", "3", "4"])
    np.testing.assert_array_equal(tsdf["static_var1"], ["A", "B", "C", "D"])
    np.testing.assert_array_equal(tsdf["static_var2"], [1.0, 2.0, 3.0, 4.0])


@pytest.mark.parametrize("backend", ["dask", "pandas", "polars", "pyarrow"])
def test_to_tswide(
    backend: str,
    request,
) -> None:
    """Test the to_wide function."""
    tslong_fixture_name = f"{backend}_tslong"
    tslong = request.getfixturevalue(tslong_fixture_name)
    tswide = tslong.to_tswide()

    assert isinstance(tswide, tswide_classes[backend])
    assert tswide._tstore_id_var == "tstore_id"
    assert tswide._tstore_time_var == "time"
    assert tswide._tstore_ts_vars == {"ts_var1": ["var1", "var2"], "ts_var2": ["var3", "var4"]}
    assert tswide._tstore_static_vars == ["static_var1", "static_var2"]
