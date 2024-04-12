## SilviMetric


SilviMetric is an open source library and set of command line utilities for extracting point cloud metrics
into a TileDB database. See https://silvimetric.com for documentation and tutorials.

[<img src="https://github.com/hobuinc/silvimetric/blob/main/docs/source/logo/Logos/PNG/SilviMeteric_Logo_2c.png?raw=true">](https://silvimetric.com/)

### Development

GitHub hosts the project at https://github.com/hobuinc/silvimetric


### Installation
These scripts will install `Silvimetric` dependencies as python libraries to
the conda environment silvimetric.

##### Pip and Conda

```
conda env create -f https://raw.githubusercontent.com/hobuinc/silvimetric/main/environment.yml
conda activate silvimetric
pip install silvimetric
```

##### Source

```
git clone https://github.com/hobuinc/silvimetric.git && cd silvimetric
conda env create -f environment.yml && conda activate silvimetric
pip install .
# To install for development:
# pip install -e .
```

### Command Line Interface

#### Base app
```
silvimetric [OPTIONS] COMMAND [ARGS]...

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
```

#### Initialize
This will create the TileDB database at the path you specify. At this point,
you will need to know the bounds, crs, resolution, and any Attributes or
Metrics your project will be leveraging. There is a default set of Attributes
and MEtrics that will be included and are documented here.

##### Usage
```
silvimetric [OPTIONS] initialize [OPTIONS]

Options:
  --bounds BOUNDS         Root bounds that encapsulates all data  [required]
  --crs CRS               Coordinate system of data  [required]
  -a, --attributes ATTRS  List of attributes to include in Database
  -m, --metrics METRICS   List of metrics to include in Database
  --resolution FLOAT      Summary pixel resolution
  --help                  Show this message and exit.
```

##### Example
```
silvimetric --database test.tdb initialize --crs "EPSG:3857" \
    --bounds '[300, 300, 600, 600]'
```

#### Scan
This will inspect a point cloud file with the database as a reference and
supply an estimate of what tile size you could use for `Shatter` operations.

##### Usage
```
silvimetric [OPTIONS]  scan [OPTIONS] POINTCLOUD

Options:
  --resolution FLOAT     Summary pixel resolution
  --filter               Remove empty space in computation. Will take extra
                         time.
  --point_count INTEGER  Point count threshold.
  --depth INTEGER        Quadtree depth threshold.
  --bounds BOUNDS        Bounds to scan.
  --help                 Show this message and exit.
```

##### Example
```
silvimetric --database test.tdb scan tests/data/test_data.copc.laz
```

#### Shatter
Shatter will read the point cloud or PDAL pipeline you've provided, and bin up
point data matching the Attributes you've requested into raster cells. After
performing this split, we'll run this point data through the Metric algorithms
defined in the initialize step, resulting in derived raster-like data.

##### Usage
```
silvimetric [OPTIONS] shatter [OPTIONS] POINTCLOUD

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
##### Example
```
silvimetric -d test.tdb shatter --date 2023-1-1 tests/data/test_data.copc.laz
```

#### Info
Info will query the database and output information about the `Shatter`
processes that been run as well as the database schema.

##### Usage
```
silvimetric [OPTIONS] info [OPTIONS]

Options:
  --bounds BOUNDS                 Bounds to filter by
  --date [%Y-%m-%d|%Y-%m-%dT%H:%M:%SZ]
                                  Select processes with this date
  --dates <DATETIME DATETIME>...  Select processes within this date range
  --name TEXT                     Select processes with this name
  --help                          Show this message and exit.
```

##### Example
```
silvimetric -d test.tdb info
```

#### Extract
Extract will take the raster-like TileDB database and output it to individual
rasters using GDAL.

##### Usage
```
silvimetric [OPTIONS] extract [OPTIONS]

Options:
  -a, --attributes ATTRS  List of attributes to include output
  -m, --metrics METRICS   List of metrics to include in output
  --bounds BOUNDS         Bounds for data to include in output
  -o, --outdir PATH       Output directory.  [required]
  --help                  Show this message and exit.
```

Note: The attributes and metrics will default to the ones available in the database.

##### Example
```
silvimetric -d test.tdb extract -o test_tifs/
```
