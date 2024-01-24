import numpy as np
import logging

from ..resources import Storage, Data, Extents, Log

def scan(tdb_dir, pointcloud, bounds, point_count, resolution):
    logger = logging.getLogger('silvimetric')
    with Storage.from_db(tdb_dir) as tdb:
        data = Data(pointcloud, tdb.config, bounds)
        extents = Extents.from_sub(tdb_dir, data.bounds)
        leaves = extents.chunk(data, resolution, point_count)

        total = np.array([l.cell_count for l in leaves])
        std = np.std(total)
        mean = np.mean(total)
        rec = int(mean + std)

        logger.info(f'Tiling information:')
        logger.info(f'  Mean tile size: {mean}')
        logger.info(f'  Std deviation: {std}')
        logger.info(f'  Recommended split size: {rec}')

        return rec