import numpy as np
import logging
import dask.bag as db
import dask
import math

from ..resources import Storage, Data, Extents, Bounds

def scan(tdb_dir: str, pointcloud: str, bounds: Bounds, point_count:int=600000, resolution:float=100,
        depth:int=6, filter:bool=False):
    """
    Scan pointcloud and determine appropriate tile sizes

    Parameters
    ----------
    tdb_dir : str
        TileDB directory path
    pointcloud : str
        Path to pointcloud
    bounds : Bounds
        Bounds filter
    point_count : int, optional
        Point count limit, by default 600000
    resolution : float, optional
        Resolution limit, by default 100
    depth : int, optional
        Tree depth limit, by default 6
    filter : bool, optional
        Remove empty extents, by default False

    Returns
    -------
    int
        Recommended tile size
    """
    logger = logging.getLogger('silvimetric')
    with Storage.from_db(tdb_dir) as tdb:

        data = Data(pointcloud, tdb.config, bounds)
        extents = Extents.from_sub(tdb_dir, data.bounds)

        if filter:
            chunks = extents.chunk(data, resolution, point_count, depth)
            breakpoint()
            cell_counts = [ch.cell_count for ch in chunks]

        else:
            cell_counts = extent_handle(extents, data, resolution, point_count,
                depth)

        std = np.std(cell_counts)
        mean = np.mean(cell_counts)
        rec = int(mean + std)

        logger.info(f'Tiling information:')
        logger.info(f'  Mean tile size: {mean}')
        logger.info(f'  Std deviation: {std}')
        logger.info(f'  Recommended split size: {rec}')

        return rec


def extent_handle(extent: Extents, data: Data, res_threshold:int=100,
        pc_threshold:int=600000, depth_threshold:int=6) -> list[int]:

    """
    Iterate through quad tree of this Extents object with given threshold parameters

    Parameters
    ----------
    extent : Extents
        Current extents
    data : Data
        Reference data object
    res_threshold : int, optional
        Resolution threshold, by default 100
    pc_threshold : int, optional
        Point count threshold, by default 600000
    depth_threshold : int, optional
        Tree depth threshold, by default 6

    Returns
    -------
    list[int]
        List of point counts
    """

    if extent.root is not None:
        bminx, bminy, bmaxx, bmaxy = extent.root.get()
        r = extent.root
    else:
        bminx, bminy, bmaxx, bmaxy = extent.bounds.get()
        r = extent.bounds

    # make bounds in scale with the desired resolution
    minx = bminx + (extent.x1 * extent.resolution)
    maxx = bminx + (extent.x2 * extent.resolution)
    miny = bmaxy - (extent.y2 * extent.resolution)
    maxy = bmaxy - (extent.y1 * extent.resolution)

    chunk = Extents(Bounds(minx, miny, maxx, maxy), extent.resolution, r)

    if extent.bounds == extent.root:
        extent.root = chunk.bounds

    curr = db.from_delayed(tile_info(chunk, data, res_threshold, pc_threshold,
            depth_threshold))
    logger = logging.getLogger('silvimetric')
    a = [ ]

    curr_depth = 0
    while curr.npartitions > 0:
        logger.info(f'Chunking {curr.npartitions} tiles at depth {curr_depth}')
        n = curr.compute()
        to_add = [ x for x in n if isinstance(x, int) ]
        a = a + to_add

        to_next = [ x for x in n if not isinstance(x, int) ]

        curr = db.from_delayed(to_next)
        curr_depth += 1

    return list(a)


@dask.delayed
def tile_info(extent: Extents, data: Data, res_threshold:int=100,
        pc_threshold:int=600000, depth_threshold:int=6, depth:int=0):
    """
    Recursively explore current extents, use thresholds to determine when to
    stop searching.

    Parameters
    ----------
    extent : Extents
        Current extents
    data : Data
        _description_
    res_threshold : int, optional
        _description_, by default 100
    pc_threshold : int, optional
        _description_, by default 600000
    depth_threshold : int, optional
        _description_, by default 6
    depth : int, optional
        _description_, by default 0

    Returns
    -------
    list[int]
        List of point counts
    """

    pc = data.estimate_count(extent.bounds)
    target_pc = pc_threshold
    minx, miny, maxx, maxy = extent.bounds.get()

    # is it empty?
    if not pc:
        return [ ]
    else:
        # has it hit the threshold yet?
        area = (maxx - minx) * (maxy - miny)
        next_split_x = (maxx-minx) / 2
        next_split_y = (maxy-miny) / 2

        # if the next split would put our area below the resolution, or if
        # the point count is less than the threshold (600k) then use this
        # tile as the work unit.
        if next_split_x < extent.resolution or next_split_y < extent.resolution:
            return [ extent.cell_count ]
        elif pc < target_pc:
            return [ extent.cell_count ]
        elif area < res_threshold**2 or depth >= depth_threshold:
            pc_per_cell = pc / (area / extent.resolution**2)
            cell_estimate = math.ceil(target_pc / pc_per_cell)
            tile_count = math.floor(extent.cell_count / cell_estimate)
            remainder = extent.cell_count % cell_estimate

            return [ cell_estimate for x in range(tile_count) ] + [ remainder ]

        else:
            return [ tile_info(ch, data, res_threshold, pc_threshold,
                    depth_threshold, depth+1) for ch in extent.split() ]