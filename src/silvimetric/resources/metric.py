import json
# import warnings
import numpy as np
from typing import Callable, Optional, Any, Union
from scipy import stats
from inspect import getsource
from tiledb import Attr
import dask
import base64
import dill

from .entry import Attribute, Entry
from . import lmom4
from . import constants

# trap warnings as errors...mainly for scipy.stats.skew and scipy.stats.kurtosis
# looks like this causes trouble for dask
# warnings.filterwarnings("error")

MetricFn = Callable[[np.ndarray, float, float, Optional[Union[Any, None]]], np.ndarray]

# Derived information about a cell of points

## TODO should create list of metrics as classes that derive from Metric?
class Metric(Entry):
    def __init__(self, name: str, method: MetricFn, dtype: np.dtype=np.float32,
            deps: list[Attribute]=None):
        super().__init__()
        self.name = name
        self._method = method
        self.dtype = dtype
        self.dependencies = deps

    def schema(self, attr: Attribute):
        entry_name = self.entry_name(attr.name)
        return Attr(name=entry_name, dtype=self.dtype)

    # common name, storage name
    def entry_name(self, attr: str) -> str:
        return f'm_{attr}_{self.name}'

    @dask.delayed
    def delayed(self, data: np.ndarray) -> np.ndarray:
        return self._method(data)

    def to_json(self) -> dict[str, any]:
        return {
            'name': self.name,
            'dtype': np.dtype(self.dtype).str,
            'dependencies': self.dependencies,
            'method_str': getsource(self._method),
            'method': base64.b64encode(dill.dumps(self._method)).decode()
        }

    def from_string(data: Union[str, dict]):
        if isinstance(data, str):
            j = json.loads(data)
        elif isinstance(data, dict):
            j = data
        name = j['name']
        dtype = np.dtype(j['dtype'])
        dependencies = j['dependencies']
        method = dill.loads(base64.b64decode(j['method'].encode()))

        return Metric(name=name, method=method, dtype=dtype, deps=dependencies)

    def __eq__(self, other):
        return (self.name == other.name and
                self.dtype == other.dtype and
                self.dependencies == other.dependencies and
                self._method == other._method)

    def __call__(self, data: np.ndarray) -> np.ndarray:
        return self._method(data)

    def __repr__(self) -> str:
        return json.dumps(self.to_json())

#TODO add all metrics from https://github.com/hobuinc/silvimetric/issues/5

def m_count(data):
    return len(data)

def m_mean(data):
    """
    Return the arithmetic mean.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return np.mean(data)

def m_mode(data):
    """
    Return the mode (most common value).

    Mode is practically undefined for floating point values. FUSION's logic is to
    partition data into 64 bins then find the bin with the highest count.
    This approach has the disadvantage that the bin size varies depending on
    the Z range.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    nbins = 64
    maxv = np.max(data)
    minv = np.min(data)
    if minv == maxv:
        return minv
    
    bins, binedges = np.histogram(data, bins = nbins, density = False)
    
    thebin = np.argmax(bins)
    
    # compute the height and return...nbins - 1 is to get the bottom value of the bin
    return minv + thebin * (maxv - minv) / (nbins - 1)

def m_median(data):
    """
    Return the median.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return np.median(data)

def m_min(data):
    """
    Return the minimum value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return np.min(data)

def m_max(data):
    """
    Return the maximum value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return np.max(data)

def m_stddev(data):
    """
    Return the standard deviation of values.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return np.std(data)

# start of new metrics to match FUSION
def m_variance(data):
    """
    Return the variance of values.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return np.var(data)

def m_cv(data):
    """
    Return the coefficient of variation of values.
    Standard deviation / mean.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return np.std(data) / np.mean(data)

# TODO check performance of other methods
def m_abovemean(data):
    """
    Return the proportion of values above the mean.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return (data > np.mean(data)).sum() / len(data)

# TODO check performance of other methods
def m_abovemode(data):
    """
    Return the proportion of values above the mode.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return (data > m_mode(data)).sum() / len(data)

# both stats.skew and stats.kurtosis throw a warning:
# Precision loss occurred in moment calculation due to catastrophic cancellation. 
# This occurs when the data are nearly identical. Results may be unreliable.
#
# I think this is because values are very similar...end up close to the mean
# GridMetrics computes these using a different formula that may also have trouble
# with numeric stability.
def m_skewness(data):
    """
    Return the skewness of values.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    if len(data) < 4:
        return NODATA

    try:
        s = stats.skew(data)
    except:
        s = NODATA

    return s

def m_kurtosis(data):
    """
    Return the kurtosis of values.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    if len(data) < 4:
        return NODATA

    try:
        k = stats.kurtosis(data)
    except:
        k = NODATA

    return k

def m_aad(data):
    """
    Return the average absolute deviation from the mean.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    m = m_mean(data)
    return np.mean(np.absolute(data - m))

def m_madmedian(data):
    """
    Return the median absolute difference from the median value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return stats.median_abs_deviation(data)

def m_madmean(data):
    """
    Return the median absolute difference from the mean value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return stats.median_abs_deviation(data, center=np.mean)

# TODO needs work
def m_madmode(data):
    """
    Return the median absolute difference from the mode value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    m = m_mode(data)
    return np.median(np.absolute(data - m))

# TODO test various methods for interpolation=... for all percentile-related metrics
# I think the default matches FUSION method but need to test
def m_iq(data):
    """
    Return the interquartile difference.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return stats.iqr(data)

def m_90m10(data):
    """
    Return the 90th percentile minus the 10th percentile.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    p = np.percentile(data, [10,90])
    return p[1] - p[0]

def m_95m05(data):
    """
    Return the 95th percentile minnus the 5th percentile.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    p = np.percentile(data, [5,95])
    return p[1] - p[0]

def m_crr(data):
    """
    Return the cannopy relief ratio.
    (mean - min) / (max - min).

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    maxv = np.max(data)
    minv = np.min(data)
    if minv == maxv:
        return NODATA
    
    return (np.mean(data) - minv) / (maxv - minv)

def m_sqmean(data):
    """
    Return the square root of the average squared value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return np.sqrt(np.mean(np.square(data)))

def m_cumean(data):
    """
    Return the cube root of the average cubed value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return np.cbrt(np.mean(np.power(np.absolute(data), 3)))

# TODO compute L-moments. These are done separately because we only add
# a single element to TileDB. This is very inefficient since we have to
# compute all L-moments at once. Ideally, we would have a single metric
# function that returns an array with 7 values

# added code to compute first 4 l-moments in lmom4.py. There is a package,
# lmoments3 that can compute the same values but I wanted to avoid the
# package as it has some IBM copyright requirements (need to include copyright
# statement with derived works)

# L1 is same as mean...compute using np.mean for speed
def m_l1(data):
    """
    Return the first L-moment (mean).

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    if len(data) < 4:
        return NODATA

    return np.mean(data)

def m_l2(data):
    """
    Return the second L-moment.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    if len(data) < 4:
        return NODATA

    l = lmom4(data)
    return l[1]

def m_l3(data):
    """
    Return the third L-moment.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    if len(data) < 4:
        return NODATA

    l = lmom4(data)
    return l[2]

def m_l4(data):
    """
    Return the fourth L-moment.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    if len(data) < 4:
        return NODATA

    l = lmom4(data)
    return l[3]

def m_lcv(data):
    """
    Return the L-moment coefficient of variation (L2 / mean).

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    if len(data) < 4:
        return NODATA

    l = lmom4(data)

    if l[0] == 0.0:
        return NODATA
    
    return l[1] / l[0]

def m_lskewness(data):
    """
    Return the L-moment skewness (L3 / L2).

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    if len(data) < 4:
        return NODATA

    l = lmom4(data)
    if l[1] == 0.0:
        return NODATA
    
    return l[2] / l[1]

def m_lkurtosis(data):
    """
    Return the L-moment kurtosis (L4 / L2).

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    if len(data) < 4:
        return NODATA

    l = lmom4(data)
    if l[1] == 0.0:
        return NODATA
    
    return l[3] / l[1]

# not sure how an array of metrics can be ingested by shatter
# so do these as 15 separate metrics. may be slower than doing all in one call
#def m_percentiles(data):
#    return(np.percentile(data, [1,5,10,20,25,30,40,50,60,70,75,80,90,95,99]))

def m_p01(data):
    """
    Return the 1st percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 1))

def m_p05(data):
    """
    Return the 5th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 5))

def m_p10(data):
    """
    Return the 10th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 10))

def m_p20(data):
    """
    Return the 20th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 20))

def m_p25(data):
    """
    Return the 25th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 25))

def m_p30(data):
    """
    Return the 30th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 30))

def m_p40(data):
    """
    Return the 40th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 40))

def m_p50(data):
    """
    Return the 50th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 50))

def m_p60(data):
    """
    Return the 60th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 60))

def m_p70(data):
    """
    Return the 70th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 70))

def m_p75(data):
    """
    Return the 75th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 75))

def m_p80(data):
    """
    Return the 80th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 80))

def m_p90(data):
    """
    Return the 90th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 90))

def m_p95(data):
    """
    Return the 95th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 95))

def m_p99(data):
    """
    Return the 99th percentile value.

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    return(np.percentile(data, 99))

def m_profilearea(data):
    """
    Return the profile area.

    Profile area was first described in 

        Hu, Tianyu, Qin Ma, Yanjun Su, John J. Battles, Brandon M. Collins, 
        Scott L. Stephens, Maggi Kelly, Qinghua Guo. 2019. A simple and 
        integrated approach for fire severity assessment using bi-temporal 
        airborne LiDAR data. Int J. Appl Earth Obs Geoinformation, 78 (2019): 25-38.

    as the area under the height percentile profile or curve. They found the 
    metric useful to compare pre- and post-fire canopy structure at both the 
    individual tree and pixel scales. The implementation in CloudMetrics and 
    GridMetrics varies from that described in Hu et al. 2019. Heights are 
    normalized using the 99th percentile height instead of the maximum height 
    to eliminate problems related to high outliers and the area under the 
    percentile curve is computed directly from 1 percent slices instead of 
    fitting a polynomial to the percentile heights and computing the area 
    under the polynomial

    Parameters
    ----------
    data : numpy.ndarray
        Data for dimension

    Returns
    -------
    float
        Value for metric computed using values in data. Returns
        NODATA value when metric cannot be computed
    """

    # sanity check...must have valid heights/elevations
    if np.max(data) <= 0:
        return NODATA


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
        return NODATA

# not sure how this will be computed once height and cover thresholds are
# sorted out
#def m_allcover(data):
#    return (data > coverthreshold).sum() / len(data) * 100.0

Metrics = {
    'count' : Metric('count', m_count),
    'mean' : Metric('mean', m_mean),
    'mode' : Metric('mode', m_mode),
    'median' : Metric('median', m_median),
    'min' : Metric('min', m_min),
    'max' : Metric('max', m_max),
    'stddev' : Metric('stddev', m_stddev),
    'variance' : Metric('variance', m_variance),
    'cv' : Metric('cv', m_cv),
    'abovemean' : Metric('abovemean', m_abovemean),
    'abovemode' : Metric('abovemode', m_abovemode),
    'skewness' : Metric('skewness', m_skewness),
    'kurtosis' : Metric('kurtosis', m_kurtosis),
    'aad' : Metric('aad', m_aad),
    'madmedian' : Metric('madmedian', m_madmedian),
    'madmode' : Metric('madmode', m_madmode),
    'iq' : Metric('iq', m_iq),
    'crr' : Metric('crr', m_crr),
    'sqmean' : Metric('sqmean', m_sqmean),
    'cumean' : Metric('cumean', m_cumean),
    'l1' : Metric('l1', m_l1),
    'l2' : Metric('l2', m_l2),
    'l3' : Metric('l3', m_l3),
    'l4' : Metric('l4', m_l4),
    'lcv' : Metric('lcv', m_lcv),
    'lskewness' : Metric('lskewness', m_lskewness),
    'lkurtosis' : Metric('lkurtosis', m_lkurtosis),
    '90m10' : Metric('90m10', m_90m10),
    '95m05' : Metric('95m05', m_95m05),
    'p01' : Metric('p01', m_p01),
    'p05' : Metric('p05', m_p05),
    'p10' : Metric('p10', m_p10),
    'p20' : Metric('p20', m_p20),
    'p25' : Metric('p25', m_p25),
    'p30' : Metric('p30', m_p30),
    'p40' : Metric('p40', m_p40),
    'p50' : Metric('p50', m_p50),
    'p60' : Metric('p60', m_p60),
    'p70' : Metric('p70', m_p70),
    'p75' : Metric('p75', m_p75),
    'p80' : Metric('p80', m_p80),
    'p90' : Metric('p90', m_p90),
    'p95' : Metric('p95', m_p95),
    'p99' : Metric('p99', m_p99),
#    'allcover' : Metric('allcover', m_allcover),
    'profilearea' : Metric('profilearea', m_profilearea),
}
