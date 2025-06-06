import numpy as np

from ..metric import Metric


def m_mean(data, *args):
    m = data.mean()
    if m.size == 0:
        return np.nan
    return m


def m_variance(data, *args):
    # copy FUSION's variance approach
    num = ((data - data.mean()) ** 2).sum()
    denom = (data.count() - 1)
    if denom == 0:
        return np.nan
    return num / denom


def m_skewness(data, *args):
    # copy FUSION's approximation of skewness
    num = ((data - data.mean()) ** 3).sum()
    denom = ( (data.count() - 1) * np.std(data) ** 3)
    if denom == 0:
        return np.nan
    return  num / denom


def m_kurtosis(data, *args):
    # copy FUSION's approximation of kurtosis
    num = ((data - data.mean()) ** 4).sum()
    denom = ((data.count() - 1) * np.std(data) ** 4)
    if denom == 0:
        return np.nan
    return num / denom


mean = Metric(name='mean', dtype=np.float32, method=m_mean)
variance = Metric(name='variance', dtype=np.float32, method=m_variance)
skewness = Metric(name='skewness', dtype=np.float32, method=m_skewness)
kurtosis = Metric(name='kurtosis', dtype=np.float32, method=m_kurtosis)

product_moments: dict[str, Metric] = dict(
    mean=mean, variance=variance, skewness=skewness, kurtosis=kurtosis
)
