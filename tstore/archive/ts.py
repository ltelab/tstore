#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 11 22:11:39 2023

@author: ghiggi
"""
import pandas as pd 
import dask.datasets
import dask.dataframe as dd


def check_time_index(df):
    return pd.api.types.is_datetime64_any_dtype(df.index.dtype)


def add_partitioning_columns(df, partitioning_str):
    """Add partitioning columns to the dataframe."""
    # TODO: as function of TS_partitioning_string (YYYY/MM/DD) or (YY/DOY/HH)
    # dayofyear, dayofweek, hour,  minute, ... 
    df["month"] = df.index.dt.month.values
    df["year"] = df.index.dt.year.values
    partition_on = ["year", "month"]
    return df, partition_on

    
def get_dataset_partitioning_columns(fpath): 
    """Infer partitioning columns from dataset file path."""
    # TODO: implement logic for that 
    partitioning_columns = ["year", "month"]
    return partitioning_columns


def ensure_is_dask_dataframe(data):
    # Ensure object is a dask dataframe 
    # - Dask Series does not have to_parquet method
    # TODO: generalize also for other object ... pandas, polars ...
    if isinstance(data, dd.Series):
        data = data.to_frame()
    return data 


class TS:
    
    def __init__(self, data):
        self.data = data
        
        
    def from_file(fpath,
                  columns=None, 
                  filters=None, 
                  split_row_groups=False,
                  calculate_divisions=True,
                  ignore_metadata_file=False,
                  **kwargs,
                  ):
        """Read a time series from disk into a Dask.DataFrame."""
        
        # Define Apache Arrow settings 
        arrow_to_pandas = {
            "zero_copy_only": False,  # Default is False. If True, raise error if doing copys
            "strings_to_categorical": False, 
            "date_as_object": False, # Default is True. If False convert to datetime64[ns]
            "timestamp_as_object": False, # Default is True. If False convert to np.datetime64[ns]
            "use_threads": True, #  parallelize the conversion using multiple threads.
            "safe": True, 
            "split_blocks": False, 
            "ignore_metadata": False,  # Default False. If False, use the ‘pandas’ metadata to get the Index
            "types_mapper": pd.ArrowDtype, # Ensure pandas is created with Arrow dtype 
        }       
        
        # Read Apache Parquet 
        df = dd.read_parquet(fpath,
                             engine="pyarrow", 
                             dtype_backend="pyarrow", 
                             index=None, # None --> Read back original time-index
                             # Filtering
                             columns=columns,  # Specify columns to load 
                             filters=filters,  # Row-filtering at read-time 
                             # Metadata options
                             calculate_divisions=calculate_divisions,  # Calculate divisions from metadata
                             ignore_metadata_file=ignore_metadata_file, # True can slowdown a lot reading
                             # Partitioning
                             split_row_groups=split_row_groups, # False --> Each file a partition
                             # Arrow options 
                             arrow_to_pandas=arrow_to_pandas, 
                             # Other options, 
                             **kwargs, 
                             )
        
        # Drop partitioning columns 
        partitioning_columns = get_dataset_partitioning_columns(fpath)
        df = df.drop(columns = partitioning_columns)     
        
        # Create the TS object
        return TS(df)
   
            
    def to_disk(self, fpath, partitioning_str=None):
        """Write TS object to disk."""
        
        # Ensure is a dask dataframe
        df = ensure_is_dask_dataframe(self.data)
        
        # Check the index is datetime
        check_time_index(df)
        
        # Check known divisions ? 
        # - TODO: behaviour to determine 
        if df.known_divisions:
            pass
            
        # Check time is sorted ?
              
        # Add partition columns 
        df, partition_on = add_partitioning_columns(df, partitioning_str)

        # Write to Parquet
        df.to_parquet(
            fpath,
            engine="pyarrow",
            # Index option 
            write_index=True,
            # Metadata
            custom_metadata=None, 
            write_metadata_file=True,  # enable writing the _metadata file
            # File structure 
            name_function = lambda i: f"part.{i}.parquet", # default naming scheme
            partition_on=partition_on,
            # Encoding
            schema="infer",
            compression='snappy', 
            # Writing options
            append=False, 
            overwrite=False, 
            ignore_divisions=False,  
            compute=True,
        )