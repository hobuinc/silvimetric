import pdal
import tiledb
import numpy as np
from math import floor

from . import Bounds

class Storage(object):
    """ Handles storage of shattered data in a TileDB structure. """
    def __init__(self, dirname, filename, config= None, cell_size=30,
                 chunk_size=16):
        self.dirname = dirname
        self.src_file = filename
        self.config = config
        self.cell_size = cell_size
        self.chunk_size = chunk_size
        ## TODO insert config here
        self.ctx = tiledb.default_ctx()

        self.atts = None
        self.schema = None
        self.tdb = None
        self.bounds = None

    def __inspect_file(self):
        """
        Does pdal quick info on source file for making informed decisions on
        storage, adding available attributes and bounds if not supplied by user.
        """
        r = pdal.Reader(self.src_file)
        info = r.pipeline().quickinfo[r.type]
        if not self.atts:
            self.atts=info['dimensions']
        if not self.bounds:
            b = info['bounds']
            srs = info['srs']['wkt']
            self.bounds = Bounds(b['minx'], b['miny'], b['maxx'], b['maxy'], self.cell_size,
                                  self.chunk_size, srs)

    def __make_schema(self, atts: list[str] = None) -> tiledb.ArraySchema:
        """
        Creates TileDB schema for Storage class.

        :param atts: list[str]
            The list of attributes to be included in TileDB
        :rtype: tiledb.ArraySchema
        :return schema
        """
        self.__inspect_file()
        dims = { d['name']: d['dtype'] for d in pdal.dimensions if d['name'] in self.atts }
        xi = floor((self.bounds.maxx - self.bounds.minx) / self.bounds.cell_size)
        yi = floor((self.bounds.maxy - self.bounds.miny) / self.bounds.cell_size)

        dim_row = tiledb.Dim(name="X", domain=(0,xi), dtype=np.float64, ctx=self.ctx)
        dim_col = tiledb.Dim(name="Y", domain=(0,yi), dtype=np.float64, ctx=self.ctx)
        domain = tiledb.Domain(dim_row, dim_col, ctx=self.ctx)

        count_att = tiledb.Attr(name="count", dtype=np.int32, ctx=self.ctx)
        tdb_atts = [tiledb.Attr(name=name, dtype=dims[name], var=True, ctx=self.ctx)
                    for name in atts]

        schema = tiledb.ArraySchema(ctx=self.ctx, domain=domain, sparse=True,
            capacity=len(atts) * xi * yi,
            attrs=[count_att, *tdb_atts], allows_duplicates=True)
        schema.check()
        return schema

    def create(self, atts:list[str] = None) -> tiledb.Config:
        """
        Creates TileDB storage.

        :param atts: list[str]
            The list of attributes to be included in TileDB
        """
        if tiledb.object_type(self.dirname) == "array":
            self.tdb = tiledb.open(self.dirname, "w")
        else:
            schema = self.__make_schema(atts)
            tiledb.SparseArray.create(self.dirname, schema)
            self.tdb = tiledb.open(self.dirname, "w")

    def write(self, xs, ys, data) -> None:
        """
        Writes to local TileDB storage.

        :param xs: ndarray(dtype=np.int32)
            List of X indices
        :param ys: ndarray(dtype=np.int32)
            List of Y indices
        :param data: ndarray(dtype=object)
            Object storage of ndarrays storing data from pointcloud
        """

        if not self.tdb:
            self.tdb = tiledb.open(self.dirname, "w")
        self.tdb[xs, ys] = data
