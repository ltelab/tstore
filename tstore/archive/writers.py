"""TStore writers."""

import yaml

from tstore.archive.io import (
    define_attributes_filepath,
    define_metadata_filepath,
)


def _write_yaml_metadata(metadata, fpath):
    """Write metadata YAML file."""
    with open(fpath, "w") as file:
        yaml.dump(metadata, file)


def write_attributes(df, base_dir):
    """Write static attributes dataframe.

    Assume df pandas !
    TODO: polars, pyarrow does not have index
    --> Ensure tstore_id column
    """
    fpath = define_attributes_filepath(base_dir)
    df.index = df.index.astype("string[pyarrow]")
    df.to_parquet(
        fpath,
        engine="pyarrow",
        compression="snappy",
        index=True,
    )


def write_metadata(base_dir, ts_variables, tstore_structure):
    """Write TStore metadata file."""
    metadata_fpath = define_metadata_filepath(base_dir)
    metadata = {}
    metadata["ts_variables"] = ts_variables
    metadata["tstore_structure"] = tstore_structure
    _write_yaml_metadata(metadata=metadata, fpath=metadata_fpath)
