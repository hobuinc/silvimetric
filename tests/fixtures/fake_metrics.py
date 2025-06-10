import numpy as np
from silvimetric import Metric


def count(data):
    return data.count()


def has_data(data):
    if data.any():
        return 1
    else:
        return 0


def metrics() -> list[Metric]:
    m1 = Metric('count', np.float32, count)
    m2 = Metric('exists', np.uint8, has_data)
    return [m1, m2]
