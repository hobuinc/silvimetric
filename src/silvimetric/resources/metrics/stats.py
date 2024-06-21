import numpy as np
from scipy import stats
from ..metric import Metric
from .p_moments import product_moments

def m_mode(data):
    u, c = np.unique(data, return_counts=True)
    i = np.where(c == c.max())
    v = u[i[0][0]]
    return v

def m_median(data):
    return np.median(data)

def m_min(data):
    return np.min(data)

def m_max(data):
    return np.max(data)

def m_stddev(data):
    return np.std(data)

def m_cv(data, stddev, mean):
    return stddev / mean

# TODO check performance of other methods
def m_abovemean(data):
    return (data > np.mean(data)).sum() / len(data)

# TODO check performance of other methods
def m_abovemode(data):
    return (data > stats.mode(data).mode).sum() / len(data)

def m_iq(data):
    return stats.iqr(data)

def m_crr(data, **kwargs):
    return (np.mean(data) - np.min(data)) / (np.max(data) - np.min(data))

def m_sqmean(data):
    return np.sqrt(np.mean(np.square(data)))

def m_cumean(data):
    return np.cbrt(np.mean(np.power(np.absolute(data), 3)))

def m_profilearea(data):
    # sanity check...must have valid heights/elevations
    if np.max(data) <= 0:
        return -9999.0

    p = np.percentile(data, range(1, 100))
    p0 = max(np.min(data), 0.0)

    # second sanity check...99th percentile must be > 0
    if p[98] > 0.0:
        # compute area under normalized percentile height curve using composite trapeziod rule
        pa = p0 / p[98]
        for ip in p[:97]:
            pa += 2.0 * ip / p[98]
        pa += 1.0

        return pa * 0.5
    else:
        return -9999.0

# # TODO example for cover using all returns and a height threshold
# # the threshold must be a parameter and not hardcoded
# def m_cover(data):
#     threshold = 2
#     return (data > threshold).sum() / len(data)

statistics: dict[str, Metric] = {}
statistics['mode'] = Metric('mode', np.float32, m_mode)
statistics['median'] = Metric('median', np.float32, m_median)
statistics['min'] = Metric('min', np.float32, m_min)
statistics['max'] = Metric('max', np.float32, m_max)
statistics['stddev'] = Metric('stddev', np.float32, m_stddev)
statistics['cv'] = Metric('cv', np.float32, m_cv, [ product_moments['mean'],
        statistics['stddev'] ])
statistics['abovemean'] = Metric('abovemean', np.float32, m_abovemean)
statistics['abovemode'] = Metric('abovemode', np.float32, m_abovemode)
statistics['iq'] = Metric('iq', np.float32, m_iq)
statistics['crr'] = Metric('crr', np.float32, m_crr)
statistics['sqmean'] = Metric('sqmean', np.float32, m_sqmean)
statistics['cumean'] = Metric('cumean', np.float32, m_cumean)
statistics['profilearea'] = Metric('profilearea', np.float32, m_profilearea)
# statistics['cover'] = Metric('cover', np.float32, m_cover)
