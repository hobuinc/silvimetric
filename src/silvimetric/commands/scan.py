import numpy as np
import logging
import dask.bag as db
import dask
import math

from ..resources import Storage, Data, Extents, Bounds, Log

def scan(tdb_dir: str, pointcloud: str, bounds: Bounds, point_count:int=600000, resolution:float=100,
        depth:int=6, filter:bool=False, log: Log=None):
    """
    Scan pointcloud and determine appropriate tile sizes.

    :param tdb_dir: TileDB database directory.
    :param pointcloud: Path to point cloud.
    :param bounds: Bounding box to filter by.
    :param point_count: Point count threshold., defaults to 600000
    :param resolution: Resolution threshold., defaults to 100
    :param depth: Tree depth threshold., defaults to 6
    :param filter: Remove empty Extents. This takes longer, but is more accurage., defaults to False
    :return: Returns list of point counts.
    """

    # TODO Scan should output other information about a file like bounds, pc, avg points per cell
    with Storage.from_db(tdb_dir) as tdb:

        if log is None:
            logger = logging.getLogger('silvimetric')
        else:
            logger = log
        data = Data(pointcloud, tdb.config, bounds)

        #TODO add this section and print out information on only parts covered
        # by the storage bounds

        # shared_bounds = Bounds.shared_bounds(data.bounds, tdb.config.root)

        # if shared_bounds != data.bounds:
        #     logger.warning('Incoming bounds is not fully within the storage'
        #         f' bounds. Adjusting incoming from {str(data.bounds)} to'
        #         f' {str(shared_bounds)}')
        #     extents = Extents.from_sub(tdb_dir, shared_bounds)
        # else:

        extents = Extents.from_sub(tdb_dir, data.bounds)

        if filter:
            chunks = extents.chunk(data, resolution, point_count, depth)
            cell_counts = [ch.cell_count for ch in chunks]

        else:
            cell_counts = extent_handle(extents, data, resolution, point_count,
                depth, log)

        count = data.count(extents.bounds)

        std = np.std(cell_counts)
        mean = np.mean(cell_counts)
        rec = int(mean + std)


        logger.info('Pointcloud information:')
        logger.info(f'  Storage Bounds: {str(tdb.config.root)}')
        logger.info(f'  Pointcloud Bounds: {str(data.bounds)}')
        logger.info(f'  Point Count: {count}')

        logger.debug('Scan thresholds:')
        logger.debug(f'  Resolution: {resolution}')
        logger.debug(f'  Point count: {point_count}')
        logger.debug(f'  Tree depth: {depth}')

        logger.info('Tiling information:')
        logger.info(f'  Mean tile size: {mean}')
        logger.info(f'  Std deviation: {std}')
        logger.info(f'  Recommended split size: {rec}')

        return rec


def extent_handle(extent: Extents, data: Data, res_threshold:int=100,
        pc_threshold:int=600000, depth_threshold:int=6, log: Log = None) -> list[int]:
    """
    Recurisvely iterate through quad tree of this Extents object with given
    threshold parameters.

    :param extent: Current Extent.
    :param data: Data object created from point cloud file.
    :param res_threshold: Resolution threshold., defaults to 100
    :param pc_threshold: Point count threshold., defaults to 600000
    :param depth_threshold: Tree depth threshold., defaults to 6
    :return: Returns list of Extents that fit thresholds.
    """

    if log is None:
        logger = logging.getLogger('silvimetric')
    else:
        logger = log

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
    a = [ ]

    curr_depth = 0
    while curr.npartitions > 0:
        logger = logging.getLogger('silvimetric')
        logger.debug(f'Chunking {curr.npartitions} tiles at depth {curr_depth}')
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

    :param extent: Current Extent.
    :param data: Data object created from point cloud file.
    :param res_threshold: Resolution threshold., defaults to 100
    :param pc_threshold: Point count threshold., defaults to 600000
    :param depth_threshold: Tree depth threshold., defaults to 6
    :param depth: Current Tree depth., defaults to 0
    :return: Returns list of Extents that fit thresholds.
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