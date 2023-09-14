import math

import dask
import pdal

class Chunk(object):
    def __init__(self, minx, maxx, miny, maxy, parent: Bounds):
        self.minx = minx
        self.maxx = maxx
        self.miny = miny
        self.maxy = maxy
        self.midx = self.minx + ((self.maxx - self.minx)/ 2)
        self.midy = self.miny + ((self.maxx - self.minx)/ 2)

        self.parent_bounds = parent
        cell_size = parent.cell_size
        group_size = parent.group_size

        self.x1 = math.floor((self.minx - parent.minx) / cell_size)
        self.x2 = math.ceil(((self.maxx - parent.minx) / cell_size))
        self.y1 = math.floor((self.miny - parent.miny) / cell_size)
        self.y2 = math.ceil(((self.maxy - parent.miny) / cell_size))

        self.bounds = Bounds(self.minx, self.miny, self.maxx, self.maxy,
                             cell_size, group_size, parent.srs.to_wkt())

        self.indices = np.array(
            [(i,j) for i in range(self.x1, self.x2+1)
            for j in range(self.y1, self.y2+1)],
            dtype=[('x', np.int32), ('y', np.int32)]
        )
        self.leaf = False

    # create quad tree of chunks for this bounds, run pdal quickinfo over this
    # chunk to determine if there are any points available in this
    # set a bottom resolution of ~1km
    def filter(self, filename):
        reader = pdal.Reader(filename)
        reader._options['bounds'] = str(self.bounds)
        pipeline = reader.pipeline()
        qi = pipeline.quickinfo[reader.type]
        pc = qi['num_points']

        if not pc:
            self.empty = True
            return

        self.empty = False

        t = 500
        if self.maxx - self.minx < t or self.maxy - self.miny < t :
            self.leaf = True
            self.set_leaf_children()
            return

        self.children = [
            Chunk(self.minx, self.midx, self.miny, self.midy, self.parent_bounds), #lower left
            Chunk(self.midx, self.maxx, self.miny, self.midy, self.parent_bounds), #lower right
            Chunk(self.minx, self.midx, self.midy, self.maxy, self.parent_bounds), #top left
            Chunk(self.midx, self.maxx, self.midy, self.maxy, self.parent_bounds)  #top right
        ]

        dask.compute([c.filter(filename) for c in self.children])

    def set_leaves(self):
        if self.leaf:
            return self
        elif not self.empty:
            leaves = dask.compute([c.set_leaves() for c in self.children])
            # if returned from local dask, access first item in tuple
            if isinstance(leaves, tuple):
                leaves = leaves[0]
            # # higher level nodes
            if isinstance(leaves, list):
                if isinstance(leaves[0], list):
                    leaves = list(chain.from_iterable(leaves))
                self.leaves = [l for l in leaves if l.leaf]

            return self.leaves

    def find_dims(self, gs):
        s = math.sqrt(gs)
        if int(s) == s:
            return [s, s]
        rng = np.arange(1, gs+1, dtype=np.int32)
        factors = rng[np.where(gs % rng == 0)]
        idx = int((len(factors)/2)-1)
        x = factors[idx]
        y = int(gs / x)
        return [x, y]

    def set_leaf_children(self):
        res = self.parent_bounds.cell_size
        gs = self.parent_bounds.group_size
        xnum, ynum = self.find_dims(gs)

        # find bounds of chunks within this chunk
        dx = da.array([[x, min(x+xnum, self.x2)] for x in range(self.x1, self.x2, xnum)], dtype=np.float64) * res + self.minx
        dy = da.array([[y, min(y+ynum, self.y2)] for y in range(self.y1, self.y2, ynum)], dtype=np.float64) * res + self.miny
        dxy = da.array([[*x,*y] for y in dy for x in dx],dtype=np.float64)
        self.children = [Chunk(*xy, parent=self.parent_bounds) for xy in dxy.compute()]