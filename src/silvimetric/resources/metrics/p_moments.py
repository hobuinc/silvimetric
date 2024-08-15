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

mean = Metric(name='mean', dtype=np.float32, method=m_mean)
moment_base = Metric(name='moment_base', dtype=object, method=m_moments, dependencies=[mean])
variance = Metric(name='variance', dtype=np.float32, method=m_variance, dependencies=[moment_base])
skewness = Metric(name='skewness', dtype=np.float32, method=m_skewness, dependencies=[moment_base])
kurtosis = Metric(name='kurtosis', dtype=np.float32, method=m_kurtosis, dependencies=[moment_base])

product_moments: dict[str, Metric] = dict(mean=mean,
    variance=variance,
    skewness=skewness,
    kurtosis=kurtosis)