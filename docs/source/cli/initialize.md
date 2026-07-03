(initialize)=

# initialize

```{only} html
Initialize constructs a SilviMetric database
```

```{Index} initialize
```

The `initialize` subcommand constructs the storage database that hosts
SilviMetric data. The backend is selected from the database URI: plain paths and
`zarr://` or `icechunk://` URIs use the Zarr/Icechunk backend, while `tiledb://`
URIs use the TileDB backend. Storage can be a local filesystem path or, for
supported backends, an object-store URI such as `s3://silvimetric/mydata`.

## Synopsis

```
Usage: silvimetric [OPTIONS] initialize [OPTIONS]

Initialize silvimetrics DATABASE

Options:
--bounds BOUNDS         Root bounds that encapsulates all data  [required]
--crs CRS               Coordinate system of data  [required]
-a, --attributes ATTRS  List of attributes to include in Database
-m, --metrics METRICS   List of metrics to include in Database
--resolution FLOAT      Summary pixel resolution
--help                  Show this message and exit.
```

## Example

Zarr/Icechunk:

```
silvimetric --database "zarr://${PWD}/test.zarr" initialize --crs "EPSG:3857" \
    --bounds '[300, 300, 600, 600]'
```

TileDB:

```
silvimetric --database "tiledb://${PWD}/test.tdb" initialize --crs "EPSG:3857" \
    --bounds '[300, 300, 600, 600]'
```
