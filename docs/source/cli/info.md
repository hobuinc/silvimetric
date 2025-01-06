(info)=

# info

```{only} html
Info will query the database and output information about the `Shatter`
processes that been run as well as the database schema.
```

```{Index} info
```

## Synopsis

```
Usage: silvimetric [OPTIONS] info [OPTIONS]

Print info about Silvimetric database

Options:
    --bounds BOUNDS                 Bounds to filter by
    --date [%Y-%m-%d|%Y-%m-%dT%H:%M:%SZ]
                                    Select processes with this date
    --history                       Show the history section of the output.
    --metadata                      Show the metadata section of the output.
    --attributes                    Show the attributes section of the output.
    --dates <DATETIME DATETIME>...  Select processes within this date range
    --name TEXT                     Select processes with this name
    --help                          Show this message and exit.
```

## Example

```
silvimetric -d test.tdb info
```