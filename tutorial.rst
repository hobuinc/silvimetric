SilviMetric Tutorial
================================================================================

:Author: Kyle Mann
:Contact: kyle@hobu.co
:Date: 2/05/2024

.. contents:: Table of Contents
   :depth: 2


This tutorial will cover how to interact with SilviMetric, including the key
commands `initialize`, `info`, `scan`, `shatter`, and `extract`. These commands
make up the core functionality of SilviMetric and will allow you convert point
cloud files into a storage system with TileDB and extract the
metrics that are produced into rasters or read with the library/language of your
choice.

Introduction
-------------------------------------------------------------------------------

SilviMetric was created as the spiritual successor to `Fusion`, a C++ library written
by Robert McGaughey with the goal of making point clouds easier to handle and
process. SilviMetric aims to handle this same problem with the use of modern
libraries, a more widely used language in `Python`. The goal is to create a
library that can be developed by a wider audience of researchers and developers,
and used on a wider variety of systems, including easier access to distributed
systems in cloud based scenarios.


Technologies
-------------------------------------------------------------------------------

Dask
................................................................................

TileDB
................................................................................

Numpy
................................................................................

Pandas
................................................................................

Command Line Interface Usage
--------------------------------------------------------------------------------

Base
................................................................................
The base options for SilviMetric include setup options that include dask setup
options, log setup optoins, and progress reporting options. The `click` python
library requires that commands and options associated with specific groups appear
in certain orders, so our base options will always be first.

```
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
```

Initialize
................................................................................
`Initialize` will create a `TileDB` database that will house all future information
that is collected about processed point clouds, including attribute data collected
about point in a cell, as well as the computed metrics for each individual
combination of `Attribute` and `Metric` for each cell.

Here you will need to define the root bounds of the data, which can be larger
than just one dataset, as well as the coordinate system it will live in. You
will also need to define any `Attributes` and `Metrics`, as these will be
propagated to future processes.

.. code-block:: console
    $ DB_NAME="western-us.tdb"
    $ BOUNDS="[-14100053.268191, 3058230.975702, -11138180.816218, 6368599.176434]"
    $ EPSG=3857

    $ silvimetric --database $DB_NAME initialize --bounds "$BOUNDS" --crs "EPSG:$EPSG"

User-Defined Metrics
................................................................................

Scan
................................................................................

.. code-block:: console
    $ FILEPATH="https://s3-us-west-2.amazonaws.com/usgs-lidar-public/MT_RavalliGraniteCusterPowder_4_2019/ept.json"
    $ silvimetric -d $DB_NAME --watch scan $FILEPATH

Output
```
2024-02-05 17:29:21,464 - silvimetric - INFO - scan:24 - Tiling information:
2024-02-05 17:29:21,465 - silvimetric - INFO - scan:25 -   Mean tile size: 447.98609121670717
2024-02-05 17:29:21,465 - silvimetric - INFO - scan:26 -   Std deviation: 38695.06897023395
2024-02-05 17:29:21,465 - silvimetric - INFO - scan:27 -   Recommended split size: 39143
```

Info
................................................................................

.. code-block:: console
    $ silvimetric -d $DB_NAME info

The output will be formatted like below.
```
2024-02-05 17:18:24,915 - silvimetric - INFO - info:153 - {
  "attributes": [
    {
      "name": "Z",
      "dtype": "<f8",
      "dependencies": null
    },
    ...
  ],
  "metadata": {
    "tdb_dir": "western-us.tdb",
    "log": {
      "logdir": null,
      "log_level": "INFO",
      "logtype": "stream",
      "logfilename": "silvimetric-log.txt"
    },
    "debug": false,
    "root": [
      -14100053.268191,
      3058230.975702,
      -11138180.816218,
      6368599.176434
    ],
    "crs": {...PROJJSON}
    "resolution": 30.0,
    "attrs": [
      {
        "name": "Z",
        "dtype": "<f8",
        "dependencies": null
      },
      ...
    ],
    "metrics": [
      {
        "name": "mean",
        "dtype": "<f4",
        "dependencies": null,
        "method_str": "def m_mean(data):\n    return np.mean(data)\n",
        "method": "gASVKwAAAAAAAACMHHNpbHZpbWV0cmljLnJlc291cmNlcy5tZXRyaWOUjAZtX21lYW6Uk5Qu"
      },
      ...
    ],
    "version": "0.0.1",
    "capacity": 1000000
  },
  "history": []
}
```

Shatter
................................................................................

.. code-block:: console
    $ bounds=(-111.44438158718762, 44.97614593664473, -111.26471853040864, 45.162953709129766)
    $ silvimetric --watch shatter $FILEPATH --tilesize 447 --date 2024-02-05 --report --bounds


Extract
................................................................................


Python API Usage
--------------------------------------------------------------------------------

Setup
................................................................................

Initialize
................................................................................

Scan
................................................................................

Info
................................................................................

Shatter
................................................................................

Extract
................................................................................


Examples
--------------------------------------------------------------------------------
