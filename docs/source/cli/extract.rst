.. _extract:

================================================================================
extract
================================================================================

.. only:: html

    Extract metrics from SilviMetrics database as raster products

.. Index:: extract


Synopsis
--------

.. code-block::

    Usage: silvimetric [OPTIONS] extract [OPTIONS]

    Extract silvimetric metrics from DATABASE

    Options:
    -a, --attributes ATTRS  List of attributes to include output
    -m, --metrics METRICS   List of metrics to include in output
    --bounds BOUNDS         Bounds for data to include in output
    -o, --outdir PATH       Output directory.  [required]
    --help                  Show this message and exit.

Example
-------

.. code-block::

    silvimetric -d test.tdb extract -o test_tifs/

.. include:: ../substitutions.txt
