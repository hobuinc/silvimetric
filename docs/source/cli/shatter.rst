.. _shatter:

================================================================================
shatter
================================================================================

.. only:: html

    Shatter inserts point cloud data into a SilviMetric database

.. Index:: shatter

Synopsis
--------

.. code-block::

    Usage: silvimetric [OPTIONS] shatter [OPTIONS] POINTCLOUD

    Insert data provided by POINTCLOUD into the silvimetric DATABASE

    Options:
    --bounds BOUNDS                 Bounds for data to include in processing
    --tilesize INTEGER              Number of cells to include per tile
    --report                        Whether or not to write a report of the
                                    process, useful for debugging
    --date [%Y-%m-%d|%Y-%m-%dT%H:%M:%SZ]
                                    Date the data was produced.
    --dates <DATETIME DATETIME>...  Date range the data was produced during
    --help                          Show this message and exit.


Example
-------

.. code-block::

    silvimetric -d test.tdb shatter --date 2023-1-1 tests/data/test_data.copc.laz


.. include:: ../substitutions.txt