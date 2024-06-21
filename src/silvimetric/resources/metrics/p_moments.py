import numpy as np
from scipy.stats import skew, kurtosis

from ..metric import Metric

def m_mean(data):
    return np.mean(data)

def m_variance(data):
    return np.var(data)

def m_skewness(data):
    return skew(data)

def m_kurtosis(data):
    return kurtosis(data)

product_moments: dict[str, Metric] = {}
product_moments['mean'] = Metric('mean', np.float32, m_mean)
product_moments['variance'] = Metric('variance', np.float32, m_variance)
product_moments['skewness'] = Metric('skewness', np.float32, m_skewness)
product_moments['kurtosis'] = Metric('kurtosis', np.float32, m_kurtosis)