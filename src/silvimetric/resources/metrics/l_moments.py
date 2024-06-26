from ..metric import Metric
import numpy as np
from .p_moments import mean
from lmoments3 import lmom_ratios

# adapted from: https://xiaoganghe.github.io/python-climate-visuals/chapters/data-analytics/scipy-basic.html
# added b3 and l4 from:
#   Vogel, R. M. and Fennessey, N. M.: L moment diagrams should replace product moment diagrams,
#   Water Resour. Res., 29, 1745–1752, https://doi.org/10.1029/93WR00341, 1993.
def lmom4(data, *args):
    # lmom4 returns the first four L-moments of data
    # data is the 1-d array
    # n is the total number of points in data, j is the j_th point
    #
    # j range in for loops starts with 1 so we need to subtract 1 for all b# equations

    data = data.values
    n = len(data)
    # sort in descending order
    data = np.sort(data.reshape(n))[::-1]

    b0 = args[0]
    l1: float = b0

    # cannot compute l-moments greater than the number of points
    if n < 2:
        l2 = np.nan
    else:
        b1 = np.array([(n - j - 1) * data[j] / n / (n - 1)
                    for j in range(n)]).sum()
        l2: float = 2 * b1 - b0

    if n < 3:
        l3 = np.nan
    else:
        b2 = np.array([(n - j - 1) * (n - j - 2) * data[j] / n / (n - 1) / (n - 2)
                    for j in range(n - 1)]).sum()
        l3: float = 6 * (b2 - b1) + b0

    if n < 4:
        l4 = np.nan
    else:
        b3 = np.array([(n - j - 1) * (n - j - 2) * (n - j - 3) * data[j] / n / (n - 1) / (n - 2) / (n - 3)
                    for j in range(n - 2)]).sum()
        l4: float = 20 * b3 - 30 * b2 + 12 * b1 - b0


    return l1, l2, l3, l4

# TODO compute L-moments. These are done separately because we only add
# a single element to TileDB. This is very inefficient since we have to
# compute all L-moments at once. Ideally, we would have a single metric
# function that returns an array with 7 values

# added code to compute first 4 l-moments in lmom4.py. There is a package,
# lmoments3 that can compute the same values but I wanted to avoid the
# package as it has some IBM copyright requirements (need to include copyright
# statement with derived works)

# L1 is same as mean...compute using np.mean for speed
def m_l1(data, *args):
    return args[0][0]

def m_l2(data, *args):
    return args[0][1]

def m_l3(data, *args):
    return args[0][2]

def m_l4(data, *args):
    return args[0][3]

def m_lcv(data, *args):
    l: tuple[float, float, float, float] = args[0]

    try:
        return l[1] / l[0]
    except ZeroDivisionError as e:
        return np.nan

def m_lskewness(data, *args):
    l: tuple[float, float, float, float] = args[0]
    try:
        return l[2] / l[1]
    except ZeroDivisionError as e:
        return np.nan

def m_lkurtosis(data, *args):
    l: tuple[float, float, float, float] = args[0]
    try:
        return l[3] / l[1]
    except ZeroDivisionError as e:
        return np.nan

# intermediate metric, not intended for insertion into db
l_mom_base = Metric('lmombase', object, lmom4, [mean])


l1 = Metric('l1', np.float32, m_l1, [l_mom_base])
l2 = Metric('l2', np.float32, m_l2, [l_mom_base])
l3 = Metric('l3', np.float32, m_l3, [l_mom_base])
l4 = Metric('l4', np.float32, m_l4, [l_mom_base])
lcv = Metric('lcv', np.float32, m_lcv, [l_mom_base])
lskewness = Metric('lskewness', np.float32, m_lskewness,
    [l_mom_base])
lkurtosis = Metric('lkurtosis', np.float32, m_lkurtosis,
    [l_mom_base])

l_moments: dict[str, Metric] = {
    'l1': l1,
    'l2': l2,
    'l3': l3,
    'l4': l4,
    'lcv': lcv,
    'lskewness': lskewness,
    'lkurtosis': lkurtosis,
}