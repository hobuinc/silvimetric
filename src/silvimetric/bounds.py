import json
import ast

class Bounds(dict): #for JSON serializing
    def __init__(self, minx: float, miny: float, maxx: float, maxy: float):
        self.minx = float(minx)
        self.miny = float(miny)
        self.maxx = float(maxx)
        self.maxy = float(maxy)

    def __eq__(self, other):
        return  other.minx == self.minx and \
                other.miny == self.miny and \
                other.maxx == self.maxx and \
                other.maxy == self.maxy

    @staticmethod
    def from_string(bbox_str: str):
        """Accepts bounds from strings in the form:
        
        "([1,101],[2,102],[3,103])"
        "{\"minx\": 1,\"miny\": 2,\"maxx\": 101,\"maxy\": 102}"
        "[1,101,2,102]"

        """
        try:
            bbox = json.loads(bbox_str)
        except json.decoder.JSONDecodeError as e:
            # try manual parsing of PDAL bounds
            # ([1,101],[2,102],[3,103])
            bbox_str = bbox_str.strip()
            if bbox_str[0] != '(':
                raise Exception(f"Unable to load Bounds via json or PDAL bounds type {e}")
            t = ast.literal_eval(bbox_str)
            minx = t[0][0]
            maxx = t[0][1]
            miny = t[1][0]
            maxy = t[1][1]
            return Bounds(minx, miny, maxx, maxy)
            
            
        # parse explicit style
        if 'minx' in bbox:
            minx = float(bbox['minx'])
            miny = float(bbox['miny'])
            maxx = float(bbox['maxx'])
            maxy = float(bbox['maxy'])
            return Bounds(minx, miny, maxx, maxy)
            
        # parse GeoJSON array style
        if len(bbox) == 4:
            return Bounds(float(bbox[0]), float(bbox[1]), float(bbox[2]),
                            float(bbox[3]))
        elif len(bbox) == 6:
            return Bounds(float(bbox[0]), float(bbox[1]), float(bbox[3]),
                            float(bbox[4]))
        else:
            raise Exception("Bounding boxes must have either 4 or 6 elements")

    def get(self) -> list[float]:
        return [self.minx, self.miny, self.maxx, self.maxy]

    def __repr__(self) -> str:
        return str(self.get())

    def to_string(self) -> str:
        return self.__repr__()
    def to_json(self) -> str:
        return json.dumps(self.get())
