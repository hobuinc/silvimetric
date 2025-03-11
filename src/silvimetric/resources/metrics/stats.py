import numpy as np
from scipy import stats
from ..metric import Metric
from .p_moments import mean
from .percentiles import pct_base
from operator import itemgetter

import warnings
# suppress warnings from dividing by 0, these are handled in the metric creation
warnings.filterwarnings(
    action='ignore',
    category=RuntimeWarning,
    module='lmoments3'
)

def m_mode(data):
    u, c = np.unique(data, return_counts=True)
    i = np.where(c == c.max())
    v = u[i[0][0]]
    return v

def m_median(data, **kwargs):
    return np.median(data)

def m_min(data, **kwargs):
    return np.min(data)

def m_max(data, **kwargs):
    return np.max(data)

def m_stddev(data, **kwargs):
    return np.std(data)

def m_cv(data, **kwargs):
    stddev, mean = itemgetter('stddev', 'mean')(kwargs)
    if mean == 0:
        return np.nan
    return stddev / mean

# TODO check performance of other methods
def m_abovemean(data, **kwargs):
    mean = kwargs['mean']
    l = len(data)
    if l == 0:
        return np.nan
    return (data > mean).sum() / l

# TODO check performance of other methods
def m_abovemode(data, **kwargs):
    mode = kwargs['mode']
    l = len(data)
    if l == 0:
        return np.nan
    return (data > mode).sum() / l

def m_iq(data):
    return stats.iqr(data)

def m_crr(data, **kwargs):
    mean = kwargs['mean']
    minimum = kwargs['min']
    maximum = kwargs['max']
    den = (maximum - minimum)
    if den == 0:
        return np.nan
    return (mean - minimum) / den

def m_sqmean(data):
    return np.sqrt(np.mean(np.square(data)))

def m_cumean(data):
    return np.cbrt(np.mean(np.power(np.absolute(data), 3)))

def m_profilearea(data, **kwargs):
    # sanity check...must have valid heights/elevations
    dmax, dmin , p = itemgetter('max', 'min', 'pct_base')(kwargs)
    if dmax <= 0:
        return -9999.0

    # p = np.percentile(data, range(1, 100))
    p0 = max(dmin, 0.0)

    # second sanity check...99th percentile must be > 0
    p99 = p[99]
    if p99 > 0.0:
        # compute area under normalized percentile height curve using composite trapeziod rule
        pcts = np.array(p[:98])
        areas = pcts * 2 / p99
        pa = p0/p99 + areas.sum() + 1

        return pa * 0.5
    else:
        return -9999.0

mode = Metric('mode', np.float32, m_mode)
median = Metric('median', np.float32, m_median)
# TODO better names?
sm_min = Metric('min', np.float32, m_min)
sm_max = Metric('max', np.float32, m_max)
stddev = Metric('stddev', np.float32, m_stddev)
cv = Metric('cv', np.float32, m_cv, [ mean, stddev ])
abovemean = Metric('abovemean', np.float32, m_abovemean, [ mean ])
abovemode = Metric('abovemode', np.float32, m_abovemode, [ mode ])
iq = Metric('iq', np.float32, m_iq)
crr = Metric('crr', np.float32, m_crr, [ mean, sm_min, sm_max ])
sqmean = Metric('sqmean', np.float32, m_sqmean)
cumean = Metric('cumean', np.float32, m_cumean)
profilearea = Metric('profilearea', np.float32, m_profilearea, [ sm_max, sm_min, pct_base ])

statistics: dict[str, Metric] = dict(
    mode=mode,
    median=median,
    min=sm_min,
    max=sm_max,
    stddev=stddev,
    cv=cv,
    abovemean=abovemean,
    abovemode=abovemode,
    iq=iq,
    crr=crr,
    sqmean=sqmean,
    cumean=cumean,
    profilearea=profilearea,
)
