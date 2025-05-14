from ..metric import Metric
import numpy as np
from scipy.stats import lmoment

import warnings

# TODO: provide link to documentation on L-Moments


def lmom4(data):
    # suppress warnings from dividing by 0
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=RuntimeWarning)
        return lmoment(data, order=[1,2,3,4], nan_policy='omit').tolist()

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
