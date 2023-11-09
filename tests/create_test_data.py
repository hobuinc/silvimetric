import pdal
import numpy as np
from math import sqrt, ceil


# Create a pointcloud dataset that follows expected paramters for data testing
# purposes. Should be:
# - Uniform density
# - have intensional holes? Maybe should make a second for this
# - have expected attributes (XYZ)
# - have nice round bounds
# - be in CRS 5070

filename = './data/test_data.copc.laz'
cell_size = 30

# has a nice square root of 300
num_points = 90000 # has different value in end after points on cell lines stripped out
split = sqrt(num_points)
interval = 1

# making it square
minx = 300
maxx = minx + split
miny = minx
maxy = maxx

pos = np.arange(minx, maxx, interval, dtype=np.float32)
positions = pos[np.where(pos % cell_size != 0)]

data = np.array([(x, y, ceil(y/cell_size), ceil(y/cell_size),
                   ceil(y/cell_size), ceil(y/cell_size))
                   for x in positions for y in positions ],
    dtype=[
        ('X', np.float32),
        ('Y', np.float32),
        ('Z', np.float32),
        ('Intensity', np.uint16),
        ('NumberOfReturns', np.uint8),
        ('ReturnNumber', np.uint8)
    ]
)

print(f'writing out to {filename}')

p: pdal.Pipeline = pdal.Pipeline(arrays=[data]) | pdal.Writer(filename, a_srs='EPSG:5070', scale_x=0.01, scale_y=0.01, scale_z=0.01)
p.execute()