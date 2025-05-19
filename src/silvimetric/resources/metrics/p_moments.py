import numpy as np

from ..metric import Metric


def m_mean(data, *args):
    m = np.mean(data)
    return m


def m_variance(data, *args):
    # copy FUSION's variance approach
    return ((data - data.mean()) ** 2).sum() / (data.count() - 1)


def m_skewness(data, *args):
    # copy FUSION's approximation of skewness
    return ((data - data.mean()) ** 3).sum() / (
        (data.count() - 1) * np.std(data) ** 3
    )


def m_kurtosis(data, *args):
    # copy FUSION's approximation of kurtosis
    return ((data - data.mean()) ** 4).sum() / (
        (data.count() - 1) * np.std(data) ** 4
    )


mean = Metric(name='mean', dtype=np.float32, method=m_mean)
variance = Metric(name='variance', dtype=np.float32, method=m_variance)
skewness = Metric(name='skewness', dtype=np.float32, method=m_skewness)
kurtosis = Metric(name='kurtosis', dtype=np.float32, method=m_kurtosis)

product_moments: dict[str, Metric] = dict(
    mean=mean, variance=variance, skewness=skewness, kurtosis=kurtosis
)
