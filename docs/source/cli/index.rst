.. _cli:

================================================================================
Command Line Interface
================================================================================


.. only:: html

    The command line interface (CLI) will facilitate your interaction with
    Silvimetric from the terminal.

.. code-block::

    Usage: silvimetric [OPTIONS] COMMAND [ARGS]...

    Options:
    -d, --database PATH             Database path
    --debug                         Print debug messages?
    --log-level TEXT                Log level (INFO/DEBUG)
    --log-dir TEXT                  Directory for log output
    --progress BOOLEAN              Report progress
    --workers INTEGER               Number of workers for Dask
    --threads INTEGER               Number of threads per worker for Dask
    --watch                         Open dask diagnostic page in default web
                                    browser.
    --dasktype [threads|processes]  What Dask uses for parallelization. For
                                    moreinformation see here https://docs.dask.o
                                    rg/en/stable/scheduling.html#local-threads
    --scheduler [distributed|local|single-threaded]
                                    Type of dask scheduler. Both are local, but
                                    are run with different dask libraries. See
                                    more here https://docs.dask.org/en/stable/sc
                                    heduling.html.
    --help                          Show this message and exit.

    Commands:
    extract     Extract silvimetric metrics from DATABASE
    info        Retrieve information on current state of DATABASE
    initialize  Create an empty DATABASE
    scan        Scan point cloud and determine the optimal tile size.
    shatter     Shatter point clouds into DATABASE cells.

.. toctree::
   :maxdepth: 1
   :caption: Commands

   extract
   shatter
   info
   initialize
   scan
