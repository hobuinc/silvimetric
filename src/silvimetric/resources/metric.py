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
from .lmom4 import lmom4

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

    def __call__(self, data: np.ndarray, htthreshold: float, coverthreshold: float) -> np.ndarray:
        return self._method(data, htthreshold, coverthreshold)

    def __repr__(self) -> str:
        return json.dumps(self.to_json())

#TODO add all metrics from https://github.com/hobuinc/silvimetric/issues/5

def m_count(data, htthreshold, coverthreshold):
    return len(data)

def m_mean(data, htthreshold, coverthreshold):
    return np.mean(data)

# mode is somewhat undefined for floating point values. FUSION logic is to
# partition data into 64 bins then find the bin with the highest count.
# This approach has the disadvantage that the bin size varies depending on
# the Z range.
# Before binning, subtract min value. Computing the scaled value involves
# the bin number * bin width (max - min / #bins) + min.
def m_mode(data, htthreshold, coverthreshold):
    nbins = 64
    maxv = np.max(data)
    minv = np.min(data)
    if minv == maxv:
        return minv
    
    bins, binedges = np.histogram(data, bins = nbins, density = False)
    
    thebin = np.argmax(bins)
    
    # compute the height and return...nbins - 1 is to get the bottom Z of the bin
    return minv + thebin * (maxv - minv) / (nbins - 1)

def m_median(data, htthreshold, coverthreshold):
    return np.median(data)

def m_min(data, htthreshold, coverthreshold):
    return np.min(data)

def m_max(data, htthreshold, coverthreshold):
    return np.max(data)

def m_stddev(data, htthreshold, coverthreshold):
    return np.std(data)

# start of new metrics to match FUSION
def m_variance(data, htthreshold, coverthreshold):
    return np.var(data)

def m_cv(data, htthreshold, coverthreshold):
    return np.std(data) / np.mean(data)

# TODO check performance of other methods
def m_abovemean(data, htthreshold, coverthreshold):
    return (data > np.mean(data)).sum() / len(data)

# TODO check performance of other methods
def m_abovemode(data, htthreshold, coverthreshold):
    return (data > m_mode(data, htthreshold, coverthreshold)).sum() / len(data)

# both stats.skew and stats.kurtosis throw a warning:
# Precision loss occurred in moment calculation due to catastrophic cancellation. 
# This occurs when the data are nearly identical. Results may be unreliable.
#
# I think this is because values are very similar...end up close to the mean
# GridMetrics computes these using a different formula that may also have trouble
# with numeric stability.
def m_skewness(data, htthreshold, coverthreshold):
    if len(data) < 4:
        return -9999.0

    try:
        s = stats.skew(data)
    except:
        s = -9999.0

    return s

def m_kurtosis(data, htthreshold, coverthreshold):
    if len(data) < 4:
        return -9999.0

    try:
        k = stats.kurtosis(data)
    except:
        k = -9999.0

    return k

def m_aad(data, htthreshold, coverthreshold):
    m = m_mean(data, htthreshold, coverthreshold)
    return np.mean(np.absolute(data - m))

def m_madmedian(data, htthreshold, coverthreshold):
    return stats.median_abs_deviation(data)

def m_madmean(data, htthreshold, coverthreshold):
    return stats.median_abs_deviation(data, center=np.mean)

# TODO needs work
def m_madmode(data, htthreshold, coverthreshold):
    m = m_mode(data, htthreshold, coverthreshold)
    return np.median(np.absolute(data - m))

# TODO test various methods for interpolation=... for all percentile-related metrics
# I think the default matches FUSION method but need to test
def m_iq(data, htthreshold, coverthreshold):
    return stats.iqr(data)

def m_90m10(data, htthreshold, coverthreshold):
    p = np.percentile(data, [10,90])
    return p[1] - p[0]

def m_95m05(data, htthreshold, coverthreshold):
    p = np.percentile(data, [5,95])
    return p[1] - p[0]

def m_crr(data, htthreshold, coverthreshold):
    maxv = np.max(data)
    minv = np.min(data)
    if minv == maxv:
        return -9999.0
    
    return (np.mean(data) - minv) / (maxv - minv)

def m_sqmean(data, htthreshold, coverthreshold):
    return np.sqrt(np.mean(np.square(data)))

def m_cumean(data, htthreshold, coverthreshold):
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
def m_l1(data, htthreshold, coverthreshold):
    if len(data) < 4:
        return -9999.0

    return np.mean(data)

def m_l2(data, htthreshold, coverthreshold):
    if len(data) < 4:
        return -9999.0

    l = lmom4(data)
    return l[1]

def m_l3(data, htthreshold, coverthreshold):
    if len(data) < 4:
        return -9999.0

    l = lmom4(data)
    return l[2]

def m_l4(data, htthreshold, coverthreshold):
    if len(data) < 4:
        return -9999.0

    l = lmom4(data)
    return l[3]

def m_lcv(data, htthreshold, coverthreshold):
    if len(data) < 4:
        return -9999.0

    l = lmom4(data)

    if l[0] == 0.0:
        return -9999.0
    
    return l[1] / l[0]

def m_lskewness(data, htthreshold, coverthreshold):
    if len(data) < 4:
        return -9999.0

    l = lmom4(data)
    if l[1] == 0.0:
        return -9999.0
    
    return l[2] / l[1]

def m_lkurtosis(data, htthreshold, coverthreshold):
    if len(data) < 4:
        return -9999.0

    l = lmom4(data)
    if l[1] == 0.0:
        return -9999.0
    
    return l[3] / l[1]

# not sure how an array of metrics can be ingested by shatter
# so do these as 15 separate metrics. may be slower than doing all in one call
#def m_percentiles(data):
#    return(np.percentile(data, [1,5,10,20,25,30,40,50,60,70,75,80,90,95,99]))

def m_p01(data, htthreshold, coverthreshold):
    return(np.percentile(data, 1))

def m_p05(data, htthreshold, coverthreshold):
    return(np.percentile(data, 5))

def m_p10(data, htthreshold, coverthreshold):
    return(np.percentile(data, 10))

def m_p20(data, htthreshold, coverthreshold):
    return(np.percentile(data, 20))

def m_p25(data, htthreshold, coverthreshold):
    return(np.percentile(data, 25))

def m_p30(data, htthreshold, coverthreshold):
    return(np.percentile(data, 30))

def m_p40(data, htthreshold, coverthreshold):
    return(np.percentile(data, 40))

def m_p50(data, htthreshold, coverthreshold):
    return(np.percentile(data, 50))

def m_p60(data, htthreshold, coverthreshold):
    return(np.percentile(data, 60))

def m_p70(data, htthreshold, coverthreshold):
    return(np.percentile(data, 70))

def m_p75(data, htthreshold, coverthreshold):
    return(np.percentile(data, 75))

def m_p80(data, htthreshold, coverthreshold):
    return(np.percentile(data, 80))

def m_p90(data, htthreshold, coverthreshold):
    return(np.percentile(data, 90))

def m_p95(data, htthreshold, coverthreshold):
    return(np.percentile(data, 95))

def m_p99(data, htthreshold, coverthreshold):
    return(np.percentile(data, 99))

def m_profilearea(data, htthreshold, coverthreshold):
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

def m_allcover(data, htthreshold, coverthreshold):
    return (data > coverthreshold).sum() / len(data) * 100.0

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
    'allcover' : Metric('allcover', m_allcover),
    'profilearea' : Metric('profilearea', m_profilearea),
}
