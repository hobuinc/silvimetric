import numpy as np
from scipy import stats
from ..metric import Metric
from .p_moments import product_moments

def m_aad(data, *args):
    mean = args[0]
    return np.mean(np.absolute(data - mean))

def m_madmedian(data, *args):
    return stats.median_abs_deviation(data)

def m_madmean(data, *args):
    return stats.median_abs_deviation(data, center=np.mean)

def m_madmode(data):
    def mode_center(data, axis):
        return stats.mode(data, axis=axis).mode
    return stats.median_abs_deviation(data, center=mode_center)

aad: dict[str, Metric] = {}
aad['aad'] = Metric('aad', np.float32, m_aad, product_moments['mean'])
aad['madmedian'] = Metric('madmedian', np.float32, m_madmedian)
aad['madmode'] = Metric('madmode', np.float32, m_madmode)