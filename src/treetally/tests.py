import dask
import pdal
import numpy as np
import itertools
import types

from .chunk import Chunk
from .bounds import Bounds
from .shatter import arrange_data

global chunklist
res = 100
gs = 16
srs = 2992
minx = 635577.79
maxx = 639003.73
miny = 848882.15
maxy = 853537.66
filename = '/Users/kmann/code/treetally/data/autzen-classified.copc.laz'
point_count = 10653336

def get_leaves(c):
    while True:
        try:
            n = next(c)
            if isinstance(n, types.GeneratorType):
                get_leaves(n)
            elif isinstance(n, Chunk):
                chunklist.append(n)
        except StopIteration:
            break

def test_leaves(leaves, chunk):
    test_minx = 0
    test_maxx = 0
    test_miny = 0
    test_maxy = 0
    errors = []
    for leaf in leaves:
        x1,x2,y1,y2 = leaf

        # init test bounds
        if test_minx == 0: test_minx=x1
        if test_maxx == 0: test_maxx=x2
        if test_miny == 0: test_miny=y1
        if test_maxy == 0: test_maxy=y2

        # test for overlapping bounds
        if x1 > test_minx and x1 < test_maxx:
            errors.append(f'Overlapping: minx: {abs(x1-test_minx)}')
        if x2 > test_minx and x1 < test_maxx:
            errors.append(f'Overlapping: maxx: {abs(x2-test_minx)}')
        if y1 > test_miny and y1 < test_maxy:
            errors.append(f'Overlapping: miny: {abs(y1-test_miny)}')
        if y2 > test_miny and y2 < test_maxy:
            errors.append(f'Overlapping: maxy: {abs(y2-test_miny)}')

        # test derived bounds against original
        if x1 < test_minx:
            test_minx = x1
        if x2 > test_maxx:
            test_maxx = x2
        if y1 < test_miny:
            test_miny = y1
        if y2 > test_maxy:
            test_maxy = y2

    errors = []
    if test_minx != chunk.minx:
        errors.append(f"test_minx: {test_minx} does not equal chunk.minx {chunk.minx}. Off by {abs(chunk.minx-test_minx)}")
    if test_maxx != chunk.maxx:
        errors.append(f"test_maxx: {test_maxx} does not equal chunk.maxx {chunk.maxx}. Off by {abs(chunk.maxx-test_maxx)}")
    if test_miny != chunk.miny:
        errors.append(f"test_miny: {test_miny} does not equal chunk.miny {chunk.miny}. Off by {abs(chunk.miny-test_miny)}")
    if test_maxy != chunk.maxy:
        errors.append(f"test_maxy: {test_maxy} does not equal chunk.maxy {chunk.maxy}. Off by {abs(chunk.maxy-test_maxy)}")

    for e in errors:
        print(e)

# sub chunks should all add up to exactly what their parent is
# original chunk will be expanded to fit the cell size
def test_chunking():
    root = Bounds(minx, miny, maxx, maxy, res, gs, srs)
    chunk = Chunk(minx, maxx, miny, maxy, root)
    leaves = chunk.get_leaf_children()

    test_leaves(leaves, chunk)


def test_pointcount():
    reader = pdal.Reader(filename)
    root = Bounds(minx, miny, maxx, maxy, res, gs, srs)
    c = Chunk(minx, maxx, miny, maxy, root)
    f = c.filter(filename)

    chunklist = []
    get_leaves(f)

    leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in chunklist])[0]
    l = [arrange_data(reader, ch, root) for leaf in leaf_procs for ch in leaf]
    counts = dask.compute(*l, optimize_graph=True)
    count = 0
    for a in counts:
        count += a.sum()
    if count != point_count:
        print(f"Point counts don't match: Expected {point_count}, got {count}")

def index_info(leaves, chunk):
    c = np.copy(leaves)
    bounds = chunk.bounds
    dx = (c[:, 0:2] - bounds.minx) / bounds.cell_size

    dy = (c[:, 2:4] - bounds.miny) / bounds.cell_size
    c[:, 0:2] = dx
    c[:, 2:4] = dy

    u, c = np.unique(c, return_counts=True, axis=0)
    dup = u[c>1]

    print("  X ")
    print("    min:", dx.min())
    print("    max:", dx.max())
    print("  Y ")
    print("    min:", dy.min())
    print("    max:", dy.max())
    if dup.any():
        print("Duplicates:", dup)

def test_filtering():

    root = Bounds(minx, miny, maxx, maxy, res, gs, srs)
    chunk = Chunk(minx, maxx, miny, maxy, root)
    f = chunk.filter(filename, 3000)

    global chunklist
    chunklist = []
    get_leaves(f)

    leaf_procs = dask.compute([leaf.get_leaf_children() for leaf in chunklist])[0]
    leaves1 = np.array([ch for leaf in leaf_procs for ch in leaf], dtype=np.float64)

    leaves2 = chunk.get_leaf_children()

    if not np.array_equal(leaves1, leaves2):
        print(f'Leaf arrays not equal. Filtered shape: {leaves1.shape}, Unfiltered shape: {leaves2.shape}')
        print("filtered info:")
        index_info(leaves1, chunk)
        print("unfiltered info:")
        index_info(leaves2, chunk)
        test_leaves(leaves1, chunk=chunk)



if __name__ == "__main__":
    dask.config.set(scheduler="single-threaded")
    # test_chunking()
    # test_pointcount()
    test_filtering()