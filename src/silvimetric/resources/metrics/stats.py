import numpy as np
from scipy import stats

from ..metric import Metric
from .p_moments import mean

def m_mode(data, *args):
    # copy FUSION's process as closely as possible
    # split into 64 bins, return highest bin count
    # if there is a tie, return first entry (lowest bin index)

    try:
        counts, bins = np.histogram(data, 64)
    except ValueError:
        try:
            counts, bins = np.histogram(data)
        except ValueError:
            return np.nan
    mode = bins[:-1][counts == counts.max()]
    return mode[0]


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
iq = Metric('iq', np.float32, m_iq)
crr = Metric('canopy_relief_ratio', np.float32, m_crr, [mean, minimum, maximum])
sqmean = Metric('sqmean', np.float32, m_sqmean)
cumean = Metric('cumean', np.float32, m_cumean)

statistics: dict[str, Metric] = dict(
    mode=mode,
    median=median,
    min=minimum,
    max=maximum,
    stddev=stddev,
    cv=cv,
    iq=iq,
    crr=crr,
    sqmean=sqmean,
    cumean=cumean,
)
