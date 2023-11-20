import argparse
import webbrowser

from os import path

import dask
from dask.distributed import Client

from .shatter import shatter
from .extract import extract

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("--out_file", type=str)
    parser.add_argument("-o", type=str)
    parser.add_argument("--tdb_dir", type=str)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--tile_size", type=int, default=16)
    parser.add_argument("--resolution", type=float, default=30)
    parser.add_argument("--polygon", type=str, default="")
    parser.add_argument("--debug", type=bool, default=False)
    parser.add_argument("--watch", type=bool, default=False)
    parser.add_argument("--attributes", nargs="+", default="Z")

    args = parser.parse_args()

    filename = args.filename

    #defaults file stem or data directory name
    if 'ept.json' in filename:
        short = path.basename(path.dirname(filename))
    elif '.copc.' in filename:
        short = path.splitext(path.splitext(path.basename(filename))[0])[0]
    else:
        short = path.splitext(path.basename(filename))[0]

    tdb_dir = args.tdb_dir
    if not tdb_dir:
        tdb_dir = short

    out_file = args.out_file or args.o
    if not out_file:
        out_file = f'data/{short}'


    threads = args.threads
    workers = args.workers
    tile_size = args.tile_size
    res = args.resolution
    poly = args.polygon

    debug = args.debug
    watch = args.watch
    atts = args.attributes

    if debug:
        dask.config.set(scheduler="single-threaded")
        shatter(filename, tdb_dir, tile_size, res, debug, polygon=poly, atts=atts)
        extract(tdb_dir, out_file)
    else:
        with Client(n_workers=workers, threads_per_worker=threads) as client:
            dask.config.set(scheduler="processes")
            client.get_versions(check=True)
            if watch:
                webbrowser.open(client.cluster.dashboard_link)
            shatter(filename, tdb_dir, tile_size, res, debug, client, poly, atts)
            extract(tdb_dir, out_file)

if __name__ == "__main__":
    main()