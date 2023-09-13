import argparse
import webbrowser
import atexit

from os import path

import dask
from dask.distributed import Client

from .shatter import shatter

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("--tdb_dir", type=str)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--group_size", type=int, default=12)
    parser.add_argument("--resolution", type=float, default=30)
    parser.add_argument("--polygon", type=str, default="")
    parser.add_argument("--debug", type=bool, default=False)
    parser.add_argument("--watch", type=bool, default=False)

    args = parser.parse_args()

    filename = args.filename

    #defaults file stem or data directory name
    tdb_dir = args.tdb_dir
    if not tdb_dir:
        if 'ept.json' in filename:
            tdb_dir= path.basename(path.dirname(filename))
        elif '.copc.' in filename:
            tdb_dir = path.splitext(path.splitext(path.basename(filename))[0])[0]
        else:
            tdb_dir = path.splitext(path.basename(filename))[0]

    threads = args.threads
    workers = args.workers
    group_size = args.group_size
    res = args.resolution
    poly = args.polygon

    debug = args.debug
    watch = args.watch

    if debug:
        dask.config.set(scheduler="single-threaded")
        shatter(filename, tdb_dir, group_size, res, debug, polygon=poly)
    else:
        with Client(n_workers=workers, threads_per_worker=threads) as client:
            if watch:
                webbrowser.open(client.cluster.dashboard_link)
            shatter(filename, tdb_dir, group_size, res, debug, client, poly, watch)

if __name__ == "__main__":
    main()