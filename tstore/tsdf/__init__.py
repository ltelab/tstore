"""TSDF."""

import pandas as pd


class TSDF(pd.DataFrame):
    """A dataframe class with additional functionality for TSArray data."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_tstore(
        self,
        base_dir,
        partition_str=None,
        tstore_structure="id-var",
        overwrite=True,
    ):
        """Write TStore from TSDF object."""
        from tstore.tsdf.writer import write_tstore

        _ = write_tstore(
            self,
            base_dir=base_dir,
            partition_str=partition_str,
            tstore_structure=tstore_structure,
            overwrite=overwrite,
        )
