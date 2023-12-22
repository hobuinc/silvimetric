.. _about:

================================================================================
About
================================================================================

Summarizing and filtering point cloud data into useful information for modeling
is challenging. In forestry applications in particular, the |FUSION| software toolkit
is often used to extract information in preparation for modeling. FUSION, however,
has a few missing features that make it

Working with Bob McGaughey and the USFS GTAC team, Kyle Mann and Howard Butler from
|HOBU|, developed the initial prototype of SilviMetric to implement an alternative
approach to computing the "GridMetrics" component of typical |FUSION| processing
pipelines.

SilviMetric does this by breaking apart the computation of metrics into three
distinct steps – :ref:`info`, :ref:`shatter`, and :ref:`extract`. SilviMetric takes
an infrastructure computing approach to the challenge by applying emerging open
source technologies that speak cloud, are nimble with data formats, and compute
in a more friendly language - |Python|.

Technologies
--------------------------------------------------------------------------------

SilviMetric stands on the shoulders of giants to provide an integrated solution to
computing rasterized point cloud metrics. These technologies include:

* |PDAL| reads point cloud content and allows users to filter or process data
  as it ingested.
* |Dask| processes tasks for :ref`shatter` and :ref:`extract` in a highly
  parallel, cloud-friendly distributed computing environment.
* |TileDB| stores metrics in cloud object stores such as |S3| in addition to
  typical filesystems.
* |Python| computes metrics and provides a diverse and convenient computing
  capability for users to easily add and extract their own metrics to the database.


.. include:: substitutions.txt

