import numpy as np
from scipy import stats

from ..metric import Metric
from .p_moments import mean
from .percentiles import pct_base

import warnings

# suppress warnings from dividing by 0, these are handled in the metric creation
warnings.filterwarnings(
    action='ignore', category=RuntimeWarning, module='lmoments3'
)


def m_mode(data, *args):
    # copy FUSION's process as closely as possible
    # split into 64 bins, get the lowest value of highest bin count

    counts, bins = np.histogram(data, 64, range=(data.min(), data.max()))
    mode = bins[:-1][counts == counts.max()]
    return mode[0].item()


def m_median(data, *args):
    return np.median(data)


def m_min(data, *args):
    return np.min(data)


def m_max(data, *args):
    return np.max(data)


def m_stddev(data, *args):
    return np.std(data)


def m_cv(data, *args):
    stddev, mean = args
    if mean == 0:
        return np.nan
    return stddev / mean


# TODO check performance of other methods
def m_abovemean(data, *args):
    mean = args[0]
    count = len(data)
    if count == 0:
        return np.nan
    return (data > mean).sum() / count


# TODO check performance of other methods
def m_abovemode(data, *args):
    mode = args[0]
    count = len(data)
    if count == 0:
        return np.nan
    return (data > mode).sum() / count


def m_iq(data):
    return stats.iqr(data)


def m_crr(data, *args):
    mean, minimum, maximum = args
    den = maximum - minimum
    if den == 0:
        return np.nan
    return (mean - minimum) / den


def m_sqmean(data):
    return np.sqrt(np.mean(np.square(data)))


def m_cumean(data):
    return np.cbrt(np.mean(np.power(np.absolute(data), 3)))


def m_profile_area(data, *args):
    # sanity check...must have valid heights/elevations
    dmax, dmin, p = args
    if dmax <= 0:
        return -9999.0

    # p = np.percentile(data, range(1, 100))
    p0 = max(dmin, 0.0)

    # second sanity check...99th percentile must be > 0
    p99 = p[99]
    if p99 > 0.0:
        # compute area under normalized percentile height curve using composite
        # trapeziod rule
        pcts = np.array(p[:98])
        areas = pcts * 2 / p99
        pa = p0 / p99 + areas.sum() + 1

        return pa * 0.5
    else:
        return -9999.0


def m_mad_median(data, *args):
    return stats.median_abs_deviation(data, nan_policy='propagate')


# TODO what to do if mode has 2 values?
def m_mad_mode(data, *args):
    return stats.median_abs_deviation(
        data, center=stats.mode, nan_policy='propagate'
    )


mode = Metric('mode', np.float32, m_mode)
median = Metric('median', np.float32, m_median)
minimum = Metric('min', np.float32, m_min)
maximum = Metric('max', np.float32, m_max)
stddev = Metric('stddev', np.float32, m_stddev)
cv = Metric('cv', np.float32, m_cv, [stddev, mean])
abovemean = Metric('abovemean', np.float32, m_abovemean, [mean])
abovemode = Metric('abovemode', np.float32, m_abovemode, [mode])
iq = Metric('iq', np.float32, m_iq)
crr = Metric('crr', np.float32, m_crr, [mean, minimum, maximum])
sqmean = Metric('sqmean', np.float32, m_sqmean)
cumean = Metric('cumean', np.float32, m_cumean)
profile_area = Metric(
    'profile_area', np.float32, m_profile_area, [maximum, minimum, pct_base]
)

statistics: dict[str, Metric] = dict(
    mode=mode,
    median=median,
    min=minimum,
    max=maximum,
    stddev=stddev,
    cv=cv,
    abovemean=abovemean,
    abovemode=abovemode,
    iq=iq,
    crr=crr,
    sqmean=sqmean,
    cumean=cumean,
    profile_area=profile_area,
)
# statistics['cover'] = Metric('cover', np.float32, m_cover)
