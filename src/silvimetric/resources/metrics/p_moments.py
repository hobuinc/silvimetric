import numpy as np
from scipy.stats import moment

from ..metric import Metric

def m_moments(data, *args):
    mean = args[0]
    return moment(data, center=mean, order=[2,3,4], nan_policy='omit').tolist()

def m_mean(data, *args):
    return np.mean(data)

def m_variance(data, *args):
    return args[0][0]

def m_skewness(data, *args):
    return args[0][1]

def m_kurtosis(data, *args):
    return args[0][2]

mean = Metric('mean', np.float32, m_mean)
moment_base = Metric('moment_base', object, m_moments, [mean])
variance = Metric('variance', np.float32, m_variance, [moment_base])
skewness = Metric('skewness', np.float32, m_skewness, [moment_base])
kurtosis = Metric('kurtosis', np.float32, m_kurtosis, [moment_base])

product_moments: dict[str, Metric] = dict(mean=mean,
    variance=variance,
    skewness=skewness,
    kurtosis=kurtosis)