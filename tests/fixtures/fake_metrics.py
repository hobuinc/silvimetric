import numpy as np
from silvimetric import Metric


def count(data):
    return data.count()


def has_data(data):
    return data.any()


def metrics() -> list[Metric]:
    m1 = Metric('count', np.float32, count)
    m2 = Metric('exists', np.dtype('?'), has_data)
    return [m1, m2]
