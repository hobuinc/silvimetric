import pdal
import numpy as np
import pyproj
from math import sqrt, ceil, floor


# Create a pointcloud dataset that follows expected paramters for data testing
# purposes. Should be:
# - Uniform density
# - have intensional holes? Maybe should make a second for this
# - have expected attributes (XYZ)
# - have nice round bounds
# - be in CRS 3857

filename = './data/test_data.copc.laz'
crs = pyproj.CRS.from_epsg(3857)
cell_size = 30

# has a nice square root of 300
num_points = 90000 # has different value in end after points on cell lines stripped out
split = sqrt(num_points)
interval = 1

# making it square
minx = 0
maxx = minx + split
miny = minx
maxy = maxx

print(f'num_points: {num_points}')
print(f'split: {split}')
print(f'minx: {minx}')
print(f'maxx: {maxx}')
print(f'miny: {miny}')
print(f'maxy: {maxy}')


positions = np.arange(minx, maxx, interval, dtype=np.float32)
positions = positions[np.where(positions % cell_size != 0)]
mod_size = 4
data = np.array([ (x, y, ceil(y/cell_size), ceil(y/cell_size), ceil(y/cell_size), ceil(y/cell_size)) for x in positions for y in positions ],
    dtype= [
        ('X', np.float32),
        ('Y', np.float32),
        ('Z', np.float32),
        ('Intensity', np.int16),
        ('NumberOfReturns', np.int8),
        ('ReturnNumber', np.int8)
    ]
)

p: pdal.Pipeline = pdal.Pipeline(arrays=[data]) | pdal.Writer(filename, a_srs='EPSG:5070')
p.execute()