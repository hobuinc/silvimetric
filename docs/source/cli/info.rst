.. _info:

================================================================================
info
================================================================================

.. only:: html

    Info will query the database and output information about the `Shatter`
    processes that been run as well as the database schema.

.. Index:: info


Synopsis
--------

.. code-block::

    Usage: silvimetric [OPTIONS] info [OPTIONS]

    Print info about Silvimetric database

    Options:
    --bounds BOUNDS                 Bounds to filter by
    --date [%Y-%m-%d|%Y-%m-%dT%H:%M:%SZ]
                                    Select processes with this date
    --dates <DATETIME DATETIME>...  Select processes within this date range
    --name TEXT                     Select processes with this name
    --help                          Show this message and exit.

Example
-------

.. code-block::

    silvimetric -d test.tdb info

.. include:: ../substitutions.txt