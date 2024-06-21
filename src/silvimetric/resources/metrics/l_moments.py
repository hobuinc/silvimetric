from ..metric import Metric
import numpy as np

# adapted from: https://xiaoganghe.github.io/python-climate-visuals/chapters/data-analytics/scipy-basic.html
# added b3 and l4 from:
#   Vogel, R. M. and Fennessey, N. M.: L moment diagrams should replace product moment diagrams,
#   Water Resour. Res., 29, 1745–1752, https://doi.org/10.1029/93WR00341, 1993.
def lmom4(data):
    # lmom4 returns the first four L-moments of data
    # data is the 1-d array
    # n is the total number of points in data, j is the j_th point
    #
    # j range in for loops starts with 1 so we need to subtract 1 for all b# equations

    data = data.values
    n = len(data)
    # sort in descending order
    data = np.sort(data.reshape(n))[::-1]
    b0 = np.mean(data)
    b1 = np.array([(n - j - 1) * data[j] / n / (n - 1)
                   for j in range(n)]).sum()
    b2 = np.array([(n - j - 1) * (n - j - 2) * data[j] / n / (n - 1) / (n - 2)
                   for j in range(n - 1)]).sum()
    b3 = np.array([(n - j - 1) * (n - j - 2) * (n - j - 3) * data[j] / n / (n - 1) / (n - 2) / (n - 3)
                   for j in range(n - 2)]).sum()
    l1 = b0
    l2 = 2 * b1 - b0
    l3 = 6 * (b2 - b1) + b0
    l4 = 20 * b3 - 30 * b2 + 12 * b1 - b0

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
def m_l1(data):
    return np.mean(data)

def m_l2(data):
    l = lmom4(data)
    return l[1]

def m_l3(data):
    l = lmom4(data)
    return l[2]

def m_l4(data):
    l = lmom4(data)
    return l[3]

def m_lcv(data):
    l = lmom4(data)
    try:
        return l[1] / l[0]
    except ZeroDivisionError as e:
        return np.nan

def m_lskewness(data):
    l = lmom4(data)
    try:
        return l[2] / l[1]
    except ZeroDivisionError as e:
        return np.nan

def m_lkurtosis(data):
    l = lmom4(data)
    try:
        return l[3] / l[1]
    except ZeroDivisionError as e:
        return np.nan

l_moments: dict[str, Metric] = { }
