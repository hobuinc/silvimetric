import numpy as np
from scipy import stats
from ..metric import Metric

def m_aad(data):
    m = np.mean(data)
    return np.mean(np.absolute(data - m))

def m_madmedian(data):
    return stats.median_abs_deviation(data)

def m_madmean(data):
    return stats.median_abs_deviation(data, center=np.mean)

def m_madmode(data):
    def mode_center(data, axis):
        return stats.mode(data, axis=axis).mode
    return stats.median_abs_deviation(data, center=mode_center)

aad: dict[str, Metric] = {}
aad['aad'] = Metric('aad', np.float32, m_aad)
aad['madmedian'] = Metric('madmedian', np.float32, m_madmedian)
aad['madmode'] = Metric('madmode', np.float32, m_madmode)