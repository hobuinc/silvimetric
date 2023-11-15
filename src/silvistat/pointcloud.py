# from . import Storage

# class Pointcloud(object):
#     """ Handle data ingesting and arrangement. """

#     def __init__(self, filename: str, storage: Storage):
#         self.filename = filename
#         self.storage = storage
    # def __inspect_file(self):
    #     """
    #     Does pdal quick info on source file for making informed decisions on
    #     storage, adding available attributes and bounds if not supplied by user.
    #     """
    #     r = pdal.Reader(self.src_file)
    #     info = r.pipeline().quickinfo[r.type]
    #     if not self.atts:
    #         self.atts=info['dimensions']
    #     if not self.bounds:
    #         b = info['bounds']
    #         srs = info['srs']['wkt']
    #         self.bounds = Bounds(b['minx'], b['miny'], b['maxx'], b['maxy'], self.resolution,
    #                               self.chunk_size, srs)
