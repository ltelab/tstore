#!/usr/bin/env python3
"""
Created on Mon Apr  8 16:31:23 2024.

@author: ghiggi
"""

from datetime import datetime
from typing import Optional, Union

from pandas._typing import IntervalClosedType

# FilterDateTimeType = datetime | str
FilterDateTimeType = Union[datetime, str]


def get_time_filters(
    *,
    start_time: Optional[FilterDateTimeType] = None,
    end_time: Optional[FilterDateTimeType] = None,
    inclusive: IntervalClosedType = "both",
) -> Union[list[tuple[str, str, FilterDateTimeType]], None]:
    """
    Define filters for Parquet Dataset subsetting at read-time.

    Parameters
    ----------
    start_time, end_time : datetime.datetime or str, optional
        Start and end time to filter the dataset. The default value of None means no
        filtering.
    inclusive : {"both", "neither", "left", "right"}, default "both"
        Define which bounds of the range are included.

    Returns
    -------
    filters : list of tuples or None
        List of tuples of the form (column_name, operator, value) to filter the dataset
        at read time. Returns None if `start_time` and `end_time` are both None.
    """
    filters = []

    if start_time is not None:
        start_op = ">"
        if inclusive in ["both", "left"]:
            start_op += "="
        filters.append(("time", start_op, start_time))
    if end_time is not None:
        end_op = "<"
        if inclusive in ["both", "right"]:
            end_op += "="
        filters.append(("time", end_op, end_time))

    if filters:
        return filters
    return None
