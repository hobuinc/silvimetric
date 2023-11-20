import json

class Bounds(object):
    def __init__(self, minx: float, miny: float, maxx: float, maxy: float):
        self.minx = float(minx)
        self.miny = float(miny)
        self.maxx = float(maxx)
        self.maxy = float(maxy)

    @staticmethod
    def from_string(bbox_str: str):
        #TODO accept more forms of bounds than just bbox array
        bbox = json.loads(bbox_str)
        if len(bbox) == 4:
            return Bounds(float(bbox[0]), float(bbox[1]), float(bbox[2]),
                            float(bbox[3]))
        elif len(bbox) == 6:
            return Bounds(float(bbox[0]), float(bbox[1]), float(bbox[3]),
                            float(bbox[4]))
        else:
            raise Exception("Bounding boxes must have either 4 or 6 elements")

    def get(self):
        return [self.minx, self.miny, self.maxx, self.maxy]
