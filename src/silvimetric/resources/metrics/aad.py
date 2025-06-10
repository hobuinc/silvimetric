import numpy as np
from ..metric import Metric
from .p_moments import product_moments, mean
from .stats import mode, median


def m_aad(data, *args):
    mean = args[0]
    return np.mean(np.absolute(data - mean))


def m_madmedian(data, *args):
    median = args[0]
    return np.median(abs(data - median))


def m_madmean(data, *args):
    mean = args[0]
    return np.median(abs(data - mean))


def m_madmode(data, *args):
    mode = args[0]
    return np.median(abs(data - mode))


aad: dict[str, Metric] = {}
aad['aad'] = Metric(
    'aad', np.float32, m_aad, dependencies=[product_moments['mean']]
)
aad['mad_median'] = Metric(
    'mad_median', np.float32, m_madmedian, dependencies=[median]
)
aad['mad_mode'] = Metric('mad_mode', np.float32, m_madmode, dependencies=[mode])
aad['mad_mean'] = Metric('mad_mean', np.float32, m_madmean, dependencies=[mean])
