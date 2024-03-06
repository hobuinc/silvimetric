.. _initialize:

================================================================================
initialize
================================================================================

.. only:: html

    Initialize constructs a SilviMetric database

.. Index:: initialize

The `initialize` subcommand constructs the basic |TileDB| instance to host the
SilviMetric data. It can be either a local filesystem path or a |S3| URI (eg.
``s3://silvimetric/mydata``).


Synopsis
--------

.. code-block::

    Usage: silvimetric DATABASE initialize [OPTIONS] BOUNDS CRS

    Initialize silvimetrics DATABASE

    Options:
    -a, --attributes ATTRS  List of attributes to include in Database
    -m, --metrics METRICS   List of metrics to include in Database
    --resolution FLOAT      Summary pixel resolution
    --htthreshold FLOAT     Height threshold for all metrics
    --coverthreshold FLOAT  Height threshold for cover metrics
    --help                  Show this message and exit.


.. include:: ../substitutions.txt