import pdal
import tiledb
import numpy as np

from . import Bounds

class Storage(object):

    def __init__(self, dirname, filename, config=None):
        self.dirname = dirname
        self.src_file = filename
        self.config = config
        self.tdb = None

    def get_atts(self, filename):
        r = pdal.Reader(filename)
        info = r.pipeline().quickinfo[r.type]
        return info['dimensions']

    def init(self, bounds: Bounds, filename: str, atts=None) -> tiledb.Config:
        dims = { d['name']: d['dtype'] for d in pdal.dimensions }
        if tiledb.object_type(self.dirname) == "array":
            with tiledb.open(self.dirname, "d") as A:
                A.query(cond="X>=0").submit()
        else:
            dim_row = tiledb.Dim(name="X", domain=(0,bounds.xi), dtype=np.float64)
            dim_col = tiledb.Dim(name="Y", domain=(0,bounds.yi), dtype=np.float64)
            domain = tiledb.Domain(dim_row, dim_col)

            count_att = tiledb.Attr(name="count", dtype=np.int32)

            if not atts:
                atts = self.get_atts(filename)
            tdb_atts = [tiledb.Attr(name=name, dtype=dims[name], var=True) for name in atts]

            schema = tiledb.ArraySchema(domain=domain, sparse=True,
                capacity=len(atts)*bounds.xi*bounds.yi, attrs=[count_att, *tdb_atts], allows_duplicates=True)
            schema.check()
            tiledb.SparseArray.create(self.dirname, schema)
            self.tdb = tiledb.open(self.dirname, "w")

        return tiledb.Config({
            "sm.check_coord_oob": False,
            "sm.check_global_order": False,
            "sm.check_coord_dedups": False,
            "sm.dedup_coords": False
        })

    def write(self, xs, ys, data) -> None:
        if not self.tdb:
            self.tdb = tiledb.open(self.dirname, "w")
        self.tdb[xs, ys] = data
