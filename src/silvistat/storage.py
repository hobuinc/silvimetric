import pdal
import tiledb
import numpy as np

from . import Bounds

class Storage(object):

    def __init__(self, dirname, filename, config=None, cell_size=30,
                 chunk_size=16):
        self.dirname = dirname
        self.src_file = filename
        self.config = config
        self.cell_size = cell_size
        self.chunk_size = chunk_size

        self.schema = None
        self.tdb = None
        self.bounds = None

    def __inspect_file(self):
        r = pdal.Reader(self.filename)
        info = r.pipeline().quickinfo[r.type]
        self.atts=info['dimensions']
        if not self.bounds:
            b = info['bounds']
            self.bounds = Bounds(b.minx, b.miny, b.maxx, b.maxy, self.cell_size,
                                  self.chunk_size)

    def __make_schema(self, atts: list[str]) -> tiledb.ArraySchema:
        dims = { d['name']: d['dtype'] for d in pdal.dimensions }
        self.__inspect_file()
        dim_row = tiledb.Dim(name="X", domain=(0,self.bounds.xi),
                             dtype=np.float64)
        dim_col = tiledb.Dim(name="Y", domain=(0,self.bounds.yi),
                             dtype=np.float64)
        domain = tiledb.Domain(dim_row, dim_col)

        count_att = tiledb.Attr(name="count", dtype=np.int32)

        tdb_atts = [tiledb.Attr(name=name, dtype=dims[name], var=True)
                    for name in atts]

        schema = tiledb.ArraySchema(domain=domain, sparse=True,
            capacity=len(atts)*self.bounds.xi*self.bounds.yi,
            attrs=[count_att, *tdb_atts], allows_duplicates=True)
        schema.check()
        return schema

    def create(self, atts=None) -> tiledb.Config:
        if tiledb.object_type(self.dirname) == "array":
            self.tdb = tiledb.open(self.dirname, "w")
        else:
            schema = self.__make_schema(atts)
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
