import time
import math
import argparse
import webbrowser

from .shatter import shatter

import dask
from dask.distributed import Client, LocalCluster

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--group_size", type=int, default=3)
    parser.add_argument("--resolution", type=float, default=30)
    parser.add_argument("--polygon", type=str, default="")
    parser.add_argument("--polygon_srs", type=str, default="EPSG:4326")
    parser.add_argument("--local", type=bool, default=False)
    parser.add_argument("--watch", type=bool, default=False)

    args = parser.parse_args()

    filename = args.filename
    threads = args.threads
    workers = args.workers
    group_size = args.group_size
    res = args.resolution
    poly = args.polygon
    p_srs = args.polygon_srs

    local = args.local
    watch = args.watch
    client = None

    if local:
        if threads == 1:
            dask.config.set(scheduler="single-threaded")
        else:
            dask.config.set(scheduler='processes')

    else:
        client = Client(n_workers=workers, threads_per_worker=threads)
        if watch:
            webbrowser.open(client.cluster.dashboard_link)

    shatter(filename, group_size, res, local, client, poly, p_srs, watch)



if __name__ == "__main__":
    main()