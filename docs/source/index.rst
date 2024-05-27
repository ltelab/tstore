.. d documentation master file, created by
   sphinx-quickstart on Wed Jul 13 14:44:07 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to the TStore documentation!
======================================

The **TStore** is a Python package designed to make your life easier when dealing with time series data. Our goal is to empower you to focus more on what you can discover and create with the data, rather than getting bogged down by the process of handling it.

With **TStore**, you can:

1. Efficiently **store** *(probably almost) any form* time series data to disk. Thanks to its hierarchically-structured approach based on `Apache Parquet <https://parquet.apache.org>`__ and `GeoParquet <https://github.com/opengeospatial/geoparquet>`__, you can choose how samples and variables are stored and temporally partitioned.

2. Process complex time series data structures thanks to the `TSDF` object, which by encapsulating univariate or multivariate time series data into an `Apache Arrow structure <https://arrow.apache.org/docs/python/data.html>`__ `TS`. Thus, you can *zero-copy transform* `TS` objects into any of the supported data frame backends (pandas, dask, polars, pyarrow). This enables *distributed, lazy and and parallel processing*. Additionally, `TSDF` have seamless integration with geopandas and xvec to perform spatial operations.

See the :ref:`key concepts <key_concepts>` page for more details on the TStore's core functionalities, and/or jump in to explore the various :ref:`tutorials <tutorials>` available in the documentation.

We're excited to see how you'll use this tool to push the boundaries of what's possible, all while making your workflow smoother, enjoyable, and more productive.


**Ready to jump in?**

Consider joining our `Slack Workspace <https://join.slack.com/t/tstore-workspace/shared_invite/zt-2g8uanpgm-dYrL6rxk5pEpAKCYn~QQ5Q>`__ to say hi or ask questions.
It's a great place to connect with others and get support.

Let's get started and unlock the full potential of TSTORE archiving together!


Documentation
=============

.. toctree::
   :maxdepth: 2

   01_key_concepts
   02_quickstart
   03_tutorials
   04_contributors_guidelines
   05_maintainers_guidelines
   06_authors


API Reference
===============

.. toctree::
   :maxdepth: 1

   API <api/modules>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
