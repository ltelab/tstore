#!/usr/bin/env python3
"""
Created on Mon Apr  8 17:05:26 2024

@author: ghiggi
"""


def get_partitioning_mapping_dict(time_var, backend="pandas"):
    # Mapping of partitioning components to corresponding pandas attributes
    if backend == "pandas":
        partitioning_mapping = {
            "year": lambda df: df[time_var].dt.year,
            "month": lambda df: df[time_var].dt.month,
            "day": lambda df: df[time_var].dt.day,
            "doy": lambda df: df[time_var].dt.dayofyear,
            "dow": lambda df: df[time_var].dt.dayofweek,
            # week TODO
            "hh": lambda df: df[time_var].dt.hour,
            "mm": lambda df: df[time_var].dt.minute,
            "ss": lambda df: df[time_var].dt.second,
        }
    elif backend == "polars":
        partitioning_mapping = {
            "year": lambda df: df[time_var].dt.year(),
            "month": lambda df: df[time_var].dt.month(),
            "day": lambda df: df[time_var].dt.day(),
            "doy": lambda df: df[time_var].dt.ordinal_day(),
            "dow": lambda df: df[time_var].dt.weekday(),
            # 'week': lambda df: df[time_var].dt.week(),
            "hh": lambda df: df[time_var].dt.hour(),
            "mm": lambda df: df[time_var].dt.minute(),
            "ss": lambda df: df[time_var].dt.second(),
        }

    else:
        raise NotImplementedError(f"Backend {backend}")
    # TODO: add quarter, daysinmonth, month_name and relevant checks
    # TODO: partitioning_str: (YYYY/MM/DD) or (YYYY/DOY/HH). Or list ?
    # TODO: provide proxy for year(YYYY) and month (MM) ? But month conflicts with minutes ?

    # TODO: for polars
    return partitioning_mapping


def get_valid_partitions():
    return list(get_partitioning_mapping_dict(time_var="dummy"))


def check_partitions(partitioning_str):
    if partitioning_str is None:
        return None

    # Parse the partitioning string to extract partitioning components
    partitioning_components = partitioning_str.split("/")

    # Get valid partitions
    valid_partitions = get_valid_partitions()

    # Check specified partitions
    partitions = []
    for component in partitioning_components:
        if component.lower() not in valid_partitions:
            raise ValueError(f"Invalid partitioning component '{component}'")
        partitions.append(component.lower())

    # Ensure month/day or doy is specified
    if "month" in partitions and "doy" in partitions:
        raise ValueError("Either specify 'month' or 'doy' (day of year).")
    if "day" in partitions and "doy" in partitions:
        raise ValueError("Either specify 'day' or 'doy' (day of year).")

    return partitions


def check_partitioning(partitioning, ts_variables):
    """Check to_tstore partitioning values."""
    if not isinstance(partitioning, (dict, str, type(None))):
        raise TypeError("")
    if isinstance(partitioning, str) or partitioning is None:
        partitioning = {ts_variable: partitioning for ts_variable in ts_variables}
    for ts_variable, partitioning_str in partitioning.items():
        try:
            partitions = check_partitions(partitioning_str)
            if partitions is not None:
                partitioning[ts_variable] = "/".join(partitions)
        except Exception as e:
            raise ValueError(f"Invalid partitioning for {ts_variable}: {e}")
    return partitioning


def add_partitioning_columns(df, partitioning_str, time_var, backend):
    """Add partitioning columns to the dataframe based on the partitioning string."""
    if partitioning_str is None:
        return df, None

    partitions = check_partitions(partitioning_str)
    partitioning_mapping = get_partitioning_mapping_dict(time_var=time_var, backend=backend)
    for component in partitions:
        if backend in ["pandas"]:
            df[component] = partitioning_mapping[component](df)
        elif backend == "polars":
            df_series = partitioning_mapping[component](df)
            df = df.with_columns(df_series.alias(component))
        else:
            raise NotImplementedError
    return df, partitions
