---
Author: Kyle Mann
Contact: <mailto:kyle@hobu.co>
Date: 2/05/2024
---

(tutorial)=

# Tutorial

```{contents} Table of Contents
:depth: 2
```

This tutorial will cover how to interact with SilviMetric, including the key
commands {ref}`initialize`, {ref}`info`, {ref}`scan`, {ref}`shatter`, and
{ref}`extract`. These commands make up the core functionality of SilviMetric
and will allow you convert point cloud files into a storage system with TileDB
and extract the metrics that are produced into rasters or read with the
library/language of your choice.

## Introduction

SilviMetric was created as a spiritual successor to {{ FUSION }}, a C++ library
written by Robert McGaughey, with a focus on the point cloud metric extraction
and management capability that FUSION provides in the form of GridMetrics.
SilviMetric aims to handle the challenge of computing statistics and metrics
from LiDAR data by using {{ Python }} instead of C++, delegate data management to
{{ TileDB }}, and leverage the wealth of capabilities provided in the machine
learning and scientific computing ecosystem of Python. The goal is to create a
library and command line utilities that a wider audience of researchers and
developers can contribute to, support distributed computing and storage systems
in the cloud, and provide a convenient solution to the challenge of
distributing and managing LiDAR metrics that are typically used for forestry
modeling.

## Technologies

### TileDB

{{ TileDB }} is an open source database engine for array data. Some features that it
provides that make it especially attractive for the challenge that SilviMetric has
include:

- Sophisticated "infinite" sparse array support
- Time travel
- Multiple APIs (Python, C++, R, Go, Java)
- Cloud object store (S3, AzB, GCS)

### PDAL

{{ PDAL }} provides point cloud processing, translation, and data conditioning utilities
that SilviMetric depends upon to read, ingest, and process point cloud data.

### Dask

{{ Dask }} provides the parallel execution engine for SilviMetric.

### NumPy

{{ NumPy }} is the array processing library of Python that other libraries in the
ecosystem build upon including {{ PDAL }}, {{ SciPy }}, {{ scikitlearn }} and
{{ PyTorch }}/{{ TensorFlow }}.

## Command Line Interface Usage

### Base

The base options for SilviMetric include setup options that include dask setup
options, log setup options, and progress reporting options. The [click](https://pypi.org/project/click/) python library requires that commands and
options associated with specific groups appear in certain orders, so our base
options will always be first.

```console
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

:::{note}
Dask distributed scheduler is currently disabled for SilviMetric
:::

### Initialize

{ref}`initialize` will create a {{ TileDB }} database that will house all future information
that is collected about processed point clouds, including attribute data collected
about point in a cell, as well as the computed metrics for each individual
combination of `Attribute` and `Metric` for each cell.

Here you will need to define the root bounds of the data, which can be larger
than just one dataset, as well as the coordinate system it will live in. You
will also need to define any `Attributes` and `Metrics`, as these will be
propagated to future processes.

Example:

```console
$ DB_NAME="western-us.tdb"
$ BOUNDS="[-14100053.268191, 3058230.975702, -11138180.816218, 6368599.176434]"
$ EPSG=3857

$ silvimetric --database $DB_NAME initialize --bounds "$BOUNDS" --crs "EPSG:$EPSG"
```

Usage:

```console
Usage: silvimetric initialize [OPTIONS]

  Initialize silvimetrics DATABASE

Options:
  --bounds BOUNDS         Root bounds that encapsulates all data  [required]
  --crs CRS               Coordinate system of data  [required]
  -a, --attributes ATTRS  List of attributes to include in Database
  -m, --metrics METRICS   List of metrics to include in output, eg. '-m
                          stats,percentiles'
  --resolution FLOAT      Summary pixel resolution
  --alignment TEXT        Pixel alignment: 'AlignToCenter' or 'AlignToCorner'
  --help                  Show this message and exit.
```

(udm)=

### User-Defined Metrics

SilviMetric supports creating custom user defined Metrics not provided by the
base software. These behave the same as provided Metrics, but can be defined
per-database.

You can create a metrics module by following the sample below, and substituting
any number of extra metrics in place of `p75` and `p90`. When looking for a
metrics module, we look for a method named `metrics`, and that it returns a
list of Metric objects. The methods that are included in these objects need to
be able to be serialized by [dill](https://pypi.org/project/dill/) in order
to be pushed and fetched to and from the database.

```{code-block} python
:linenos: true

 import numpy as np

 from silvimetric.resources import Metric

 def metrics() -> list[Metric]:

     def p75(arr: np.ndarray):
         return np.percentile(arr, 75)
     m_p75 = Metric(name='p75', dtype=np.float32, method = p75)

     def p90(arr: np.ndarray):
         return np.percentile(arr, 90)
     m_p90 = Metric(name='p90', dtype=np.float32, method = p90)

     return [m_p75, m_p90]
```

When including the metrics in the `initialize` step, be sure to include them by
doing `-m './path/to/metrics.py'`. At this point, you will need to have all
other metrics you would like to include as well.

Example:

```console
$ METRIC_PATH="./path/to/python_metrics.py"
$ silvimetric --database $DB_NAME initialize --bounds "$BOUNDS" \
      --crs "EPSG:$EPSG" \
      -m "${METRIC_PATH},min,max,mean"
```

:::{warning}
Additional Metrics cannot be added to a SilviMetric after it has been
initialized at this time.
:::

### Scan

{ref}`scan` will allow us to look through the nodes of the point cloud file
that you'd like to run in order to determine a good number of cells you should
include per tile. The `Shatter` process will take steps to try to inform itself
of the best splits possible before doing it's work. The filter process will remove
any sections of the bounds that are empty before we get to the shatter process,
removing some wasted compute time. By performing a scan ahead of time though,
you only need to do it once.

When scan looks through each section of the data, it looks to see how many points
are here, how much area this section is taking up, and what depth we're at in
the octree. If any of these pass the defined thresholds, then we stop splitting
and return that tile. The number of cells in that tile further tells us how best
to split the data.

One standard deviation from the mean is a good starting point for a shatter
process, but this won't always be perfect for your use case.

Usage:

```console
Usage: silvimetric scan [OPTIONS] POINTCLOUD

  Scan point cloud, output information on it, and determine the optimal tile
  size.

Options:
  --resolution FLOAT     Summary pixel resolution
  --filter_empty         Remove empty space in computation. Will take extra
                         time.
  --point_count INTEGER  Point count threshold.
  --depth INTEGER        Quadtree depth threshold.
  --bounds BOUNDS        Bounds to scan.
  --help                 Show this message and exit.
```

Example:

```console
$ FILEPATH="https://s3-us-west-2.amazonaws.com/usgs-lidar-public/MT_RavalliGraniteCusterPowder_4_2019/ept.json"
$ silvimetric -d $DB_NAME --watch scan $FILEPATH
```

### Shatter

{ref}`shatter` is where the vast majority of the processing happens. Here
SilviMetric will take all the previously defined variables like the bounds,
resolution, and our tile size, and it will split all data values up into their
respective bins. From here, SilviMetric will perform each `Metric` previously
defined in {ref}`initialize` over the data in each cell. At the end of all that,
this data will be written to a `SparseArray` in `TileDB`, where it will be much easier to access.

Usage:

```console
Usage: silvimetric shatter [OPTIONS] POINTCLOUD

Options:
  --bounds BOUNDS                 Bounds for data to include in processing
  --tilesize INTEGER              Number of cells to include per tile
  --report                        Whether or not to write a report of the
                                  process, useful for debugging
  --date [%Y-%m-%d|%Y-%m-%dT%H:%M:%SZ]
                                  Date the data was produced.
  --dates <DATETIME DATETIME>...  Date range the data was produced during
  --help                          Show this message and exit.
```

Example:

```console
$ BOUNDS='[-12317431.810079003, 5623829.111356639, -12304931.810082098, 5642881.670239899]'
$ silvimetric -d $DB_NAME \
      shatter $FILEPATH \
      --tilesize 100 \
      --date 2024-02-05 \
      --bounds $BOUNDS
```

### Info

{ref}`info` provides the ability to inspect the SilviMetric database. Here you
can see past `Shatter` processes that have been run, including point counts,
attributes, metrics, and other process metadata.

Usage:

```console
Usage: silvimetric info [OPTIONS]

Options:
  --bounds BOUNDS                 Bounds to filter by
  --date [%Y-%m-%d|%Y-%m-%dT%H:%M:%SZ]
                                  Select processes with this date
  --dates <DATETIME DATETIME>...  Select processes within this date range
  --name TEXT                     Select processes with this name
  --help                          Show this message and exit.
```

Example:

```console
$ silvimetric -d $DB_NAME info
```

Output:

```{code-block} json
:linenos: true

{
    "attributes": [
        {
            "name": "Z",
            "dtype": "<f8",
            "dependencies": null
        }
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
        "crs": {"PROJJSON"}
        "resolution": 30.0,
        "attrs": [
            {
                "name": "Z",
                "dtype": "<f8",
                "dependencies": null
            }
        ],
        "metrics": [
            {
                "name": "mean",
                "dtype": "<f4",
                "dependencies": null,
                "method_str": "def m_mean(data):\n    return np.mean(data)\n",
                "method": "gASVKwAAAAAAAACMHHNpbHZpbWV0cmljLnJlc291cmNlcy5tZXRyaWOUjAZtX21lYW6Uk5Qu"
            }
        ],
        "version": "0.0.1",
        "capacity": 1000000
    },
    "history": []
}
```

### Extract

{ref}`extract` is the final stop, where SilviMetric outputs the metrics that
been binned up nicely, and will output them as rasters to where you select.

Usage:

```console
Usage: silvimetric extract [OPTIONS]

Extract silvimetric metrics from DATABASE

Options:
  -a, --attributes ATTRS  List of attributes to include output
  -m, --metrics METRICS   List of metrics to include in output
  --bounds BOUNDS         Bounds for data to include in output
  -o, --outdir PATH       Output directory.  [required]
  --help                  Show this message and exit.
```

Example:

```console
$ OUT_DIR="western-us-tifs"
$ silvimetric -d $DB_NAME extract --outdir $OUT_DIR
```

## Python API Usage

Everything that can be done from the command line can also be performed from
within Python. The CLI provides some nice wrapping around some of the setup
pieces, including config, log, and Dask handling, but all of these are pieces
that you can set up on your own as well.

```python
import os
from pathlib import Path
import numpy as np
import pdal
import json
import datetime
from shutil import rmtree

from silvimetric import Storage, Metric, Bounds, Pdal_Attributes
from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
from silvimetric import scan, extract, shatter
from silvimetric.resources.metrics.stats import minimum, maximum, mean

########## Setup #############

# Here we create a path for our current working directory, as well as the path
# to our forest data, the path to the database directory, and the path to the
# directory that will house the raster data.
curpath = Path(os.path.dirname(os.path.realpath(__file__)))
filename = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/MT_RavalliGraniteCusterPowder_4_2019/ept.json"
db_dir_path = Path(curpath  / "western_us.tdb")

db_dir = db_dir_path.as_posix()
out_dir = (curpath / "westsern_us_tifs").as_posix()
resolution = 30 # 30 meter resolution

# we'll use PDAL python bindings to find the srs of our data, and the bounds
reader = pdal.Reader(filename)
p = reader.pipeline()
qi = p.quickinfo[reader.type]
bounds = Bounds.from_string((json.dumps(qi['bounds'])))
srs = json.dumps(qi['srs']['json'])

######## Create Metric ########
# Metrics give you the ability to define methods you'd like applied to the data
# Here we define, the name, the data type, and what values we derive from it.

def make_metric():
    def p75(arr: np.ndarray):
        return np.percentile(arr, 75)

    return Metric(name='p75', dtype=np.float32, method = p75)

###### Create Storage #####
# This will create a tiledb database, same as the `initialize` command would
# from the command line. Here we'll define the overarching bounds, which may
# extend beyond the current dataset, as well as the CRS of the data, the list
# of attributes that will be used, as well as metrics. The config will be stored
# in the database for future processes to use.

def db():
    perc_75 = make_metric()
    attrs = [
        Pdal_Attributes[a]
        for a in ['Z', 'Intensity']
    ]
    metrics = [ mean, maximum, minimum ]
    metrics.append(perc_75)
    st_config = StorageConfig(root=bounds, resolution=resolution, crs=srs,
        attrs=attrs, metrics=metrics, tdb_dir=db_dir)
    storage = Storage.create(st_config)

####### Perform Scan #######
# The Scan step will perform a search down the resolution tree of the COPC or
# EPT file you've supplied and will provide a best guess of how many cells per
# tile you should use for this dataset.

def sc(b):
    return scan(tdb_dir=db_dir, pointcloud=filename, bounds=b)

###### Perform Shatter #####
# The shatter process will pull the config from the database that was previously
# made and will populate information like CRS, Resolution, Attributes, and what
# Metrics to perform from there. This will split the data into cells, perform
# the metric method over each cell, and then output that information to TileDB

def sh(b, tile_size):
    sh_config = ShatterConfig(tdb_dir=db_dir, date=datetime.datetime.now(),
        filename=filename, tile_size=tile_size, bounds=b)
    shatter(sh_config)

###### Perform Extract #####
# The Extract step will pull data from the database for each metric/attribute combo
# and store it in an array, where it will be output to a raster with the name
# `m_{Attr}_{Metric}.tif`. By default, each computed metric will be written
# to the output directory, but you can limit this by defining which Metric names
# you would like
def ex():
    ex_config = ExtractConfig(tdb_dir=db_dir, out_dir=out_dir)
    extract(ex_config)



if __name__ == "__main__":
    rmtree(db_dir)
    make_metric()
    db()

    # this is a large dataset, and silvimetric supports incremental insertion
    # so we'll be splitting up our bounds and shattering them that way.
    for b2 in bounds.bisect():
        for b1 in b2.bisect():
            print(f"Processing bounds: {b1}")
            scan_info = sc(b1)
            # grab the tile size for this chunk so we can operate at an apropriate rate
            # the variance in tile density leads to a large standard deviation,
            # so we'll just use the mean for this scenario
            tile_size = int(scan_info['tile_info']['mean'])
            sh(b1, tile_size)
            print(f"Finished bounds: {b1}\n")

    ex()
```

```{eval-rst}
.. include:: ./substitutions.txt
```
