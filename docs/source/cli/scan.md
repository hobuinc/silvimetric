(scan)=

# scan

```{only} html
This will inspect a point cloud file with the database as a reference and
supply an estimate of what tile size you could use for `Shatter` operations.
```

```{Index} scan
```

## Synopsis

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

## Usage

Zarr/Icechunk:

```
silvimetric --database "zarr://${PWD}/test.zarr" scan tests/data/test_data.copc.laz
```

TileDB:

```
silvimetric --database "tiledb://${PWD}/test.tdb" scan tests/data/test_data.copc.laz
```
