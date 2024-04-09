"""TSDF."""

import pandas as pd


class TSDF(pd.DataFrame):
    """A dataframe class with additional functionality for TSArray data."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_tstore(
        self,
        base_dir,
        id_var,
        time_var,  # likely not needed !
        partitioning=None,  
        tstore_structure="id-var",
        overwrite=True,  # append functionality? 
        # geometry
    ):
        """Write TStore from TSDF object."""
        from tstore.tsdf.writer import write_tstore

        _ = write_tstore(
            self,
            base_dir=base_dir,
            id_var=id_var,
            time_var=time_var,
            partitioning=partitioning,
            tstore_structure=tstore_structure,
            overwrite=overwrite,
        )
    
    # Method that return identifier column 
    
    # Method that return the timeseries columns  (TSArrays)
    
    # Add compute method 
    
    # Add wrappers to methods iloc, loc or join to return TSDF class
       
    # Remove methods that are not supported by TSArray 
    # --> min, ... 