from ..metric import Metric
import numpy as np


def lmom4(data, *args):
    """
    Create approximations of first 4 L-Moments of the cell (sample) data.
    Adapted from https://xiaoganghe.github.io/python-climate-visuals/chapters/data-analytics/scipy-basic.html
    """

    n = len(data)
    idx = np.arange(n)

    # sort in descending order
    data = np.sort(data.reshape(n))[::-1]

    b0 = data.mean()
    l1: float = b0

    # cannot compute l-moments greater than the number of points
    if n < 2:
        l2 = np.nan
    else:
        b1 = (data * (n - idx - 1) / n / (n - 1)).sum()
        l2: float = 2 * b1 - b0

    if n < 3:
        l3 = np.nan
    else:
        b2_data = data[:-1]
        b2_idx = idx[:-1]
        b2 = (
            b2_data
            * (n - b2_idx - 1)
            * (n - b2_idx - 2)
            / (n * (n - 1) * (n - 2))
        ).sum()
        l3: float = 6 * (b2 - b1) + b0

    if n < 4:
        l4 = np.nan
    else:
        b3_data = b2_data[:-1]
        b3_idx = b2_idx[:-1]
        b3 = (
            b3_data
            * (n - b3_idx - 1)
            * (n - b3_idx - 2)
            * (n - b3_idx - 3)
            / (n * (n - 1) * (n - 2) * (n - 3))
        ).sum()
        l4: float = 20 * b3 - 30 * b2 + 12 * b1 - b0

    return l1, l2, l3, l4


def m_l1(data, *args):
    return args[0][0]


def m_l2(data, *args):
    return args[0][1]


def m_l3(data, *args):
    return args[0][2]


def m_l4(data, *args):
    return args[0][3]


def m_lcv(data, *args):
    lmom: tuple[float, float, float, float] = args[0]

    try:
        return lmom[1] / lmom[0]
    except ZeroDivisionError:
        return np.nan


def m_lskewness(data, *args):
    lmom: tuple[float, float, float, float] = args[0]
    try:
        return lmom[2] / lmom[1]
    except ZeroDivisionError:
        return np.nan


def m_lkurtosis(data, *args):
    lmom: tuple[float, float, float, float] = args[0]
    try:
        return lmom[3] / lmom[1]
    except ZeroDivisionError:
        return np.nan


# intermediate metric, not intended for insertion into db
l_mom_base = Metric('lmombase', object, lmom4, [])


l1 = Metric('l1', np.float32, m_l1, [l_mom_base])
l2 = Metric('l2', np.float32, m_l2, [l_mom_base])
l3 = Metric('l3', np.float32, m_l3, [l_mom_base])
l4 = Metric('l4', np.float32, m_l4, [l_mom_base])
lcv = Metric('lcv', np.float32, m_lcv, [l_mom_base])
lskewness = Metric('lskewness', np.float32, m_lskewness, [l_mom_base])
lkurtosis = Metric('lkurtosis', np.float32, m_lkurtosis, [l_mom_base])

l_moments: dict[str, Metric] = {
    'l1': l1,
    'l2': l2,
    'l3': l3,
    'l4': l4,
    'lcv': lcv,
    'lskewness': lskewness,
    'lkurtosis': lkurtosis,
}
