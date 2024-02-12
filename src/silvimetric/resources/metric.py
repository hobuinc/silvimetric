import json
import numpy as np
from typing import Callable, Optional, Any, Union
from scipy import stats
from inspect import getsource
from tiledb import Attr
import dask
import base64
import pickle
import dill

from .entry import Attribute, Entry

MetricFn = Callable[[np.ndarray, Optional[Union[Any, None]]], np.ndarray]

# Derived information about a cell of points

## TODO should create list of metrics as classes that derive from Metric?
class Metric(Entry):
    def __init__(self, name: str, dtype: np.dtype, method: MetricFn, deps: list[Attribute]=None):
        super().__init__()
        self.name = name
        self.dtype = dtype
        self.dependencies = deps
        self._method = method

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

        return Metric(name, dtype, method, dependencies)

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

def m_mean(data):
    return np.mean(data)

def m_mode(data):
    return stats.mode(data).mode

def m_median(data):
    return np.median(data)

def m_min(data):
    return np.min(data)

def m_max(data):
    return np.max(data)

def m_stddev(data):
    return np.std(data)

# start of new metrics to match FUSION
def m_variance(data):
    return np.var(data)

def m_cv(data):
    return np.std(data) / np.mean(data)

# TODO check performance of other methods
def m_abovemean(data):
    return (data > np.mean(data)).sum() / len(data)

# TODO check performance of other methods
def m_abovemode(data):
    return (data > stats.mode(data).mode).sum() / len(data)

def m_skewness(data):
    return stats.skew(data)

def m_kurtosis(data):
    return stats.kurtosis(data)

def m_aad(data):
    m = np.mean(data)
    return np.mean(np.absolute(data - m))

def m_madmedian(data):
    return stats.median_abs_deviation(data)

def m_madmean(data):
    return stats.median_abs_deviation(data, center=np.mean)

def m_madmode(data):
    return stats.median_abs_deviation(data, center=stats.mode)

# TODO test various methods for interpolation=... I think the default
# matches FUSION method
def m_iq(data):
    return stats.iqr(data)

def m_90m10(data):
    p = np.percentile(data, [10,90])
    return p[1] - p[0]

def m_95m05(data):
    p = np.percentile(data, [5,95])
    return p[1] - p[0]

# TODO test various methods for interpolation=... I think the default
# matches FUSION method
# not sure how an array of metrics will be ingested by shatter
# so do these as 15 separate metrics. may be slower than doing all in one call
#def m_percentiles(data):
#    return(np.percentile(data, [1,5,10,20,25,30,40,50,60,70,75,80,90,95,99]))

def m_p01(data):
    return(np.percentile(data, 1))

def m_p05(data):
    return(np.percentile(data, 5))

def m_p10(data):
    return(np.percentile(data, 10))

def m_p20(data):
    return(np.percentile(data, 20))

def m_p25(data):
    return(np.percentile(data, 25))

def m_p30(data):
    return(np.percentile(data, 30))

def m_p40(data):
    return(np.percentile(data, 40))

def m_p50(data):
    return(np.percentile(data, 50))

def m_p60(data):
    return(np.percentile(data, 60))

def m_p70(data):
    return(np.percentile(data, 70))

def m_p75(data):
    return(np.percentile(data, 75))

def m_p80(data):
    return(np.percentile(data, 80))

def m_p90(data):
    return(np.percentile(data, 90))

def m_p95(data):
    return(np.percentile(data, 95))

def m_p99(data):
    return(np.percentile(data, 99))

#TODO change to correct dtype
#TODO not sure what to do with percentiles since it is an array of values instead of a single value
Metrics = {
    'mean' : Metric('mean', np.float32, m_mean),
    'mode' : Metric('mode', np.float32, m_mode),
    'median' : Metric('median', np.float32, m_median),
    'min' : Metric('min', np.float32, m_min),
    'max' : Metric('max', np.float32, m_max),
    'stddev' : Metric('stddev', np.float32, m_stddev),
    'variance' : Metric('variance', np.float32, m_variance),
    'cv' : Metric('cv', np.float32, m_cv),
    'abovemean' : Metric('abovemean', np.float32, m_abovemean),
    'abovemode' : Metric('abovemode', np.float32, m_abovemode),
    'skewness' : Metric('skewness', np.float32, m_skewness),
    'kurtosis' : Metric('kurtosis', np.float32, m_kurtosis),
    'aad' : Metric('aad', np.float32, m_aad),
    'madmedian' : Metric('madmedian', np.float32, m_madmedian),
    'madmode' : Metric('madmode', np.float32, m_madmode),
    'iq' : Metric('iq', np.float32, m_iq),
    '90m10' : Metric('90m10', np.float32, m_90m10),
    '95m05' : Metric('95m05', np.float32, m_95m05),
    'p01' : Metric('p01', np.float32, m_p01),
    'p05' : Metric('p05', np.float32, m_p05),
    'p10' : Metric('p10', np.float32, m_p10),
    'p20' : Metric('p20', np.float32, m_p20),
    'p25' : Metric('p25', np.float32, m_p25),
    'p30' : Metric('p30', np.float32, m_p30),
    'p40' : Metric('p40', np.float32, m_p40),
    'p50' : Metric('p50', np.float32, m_p50),
    'p60' : Metric('p60', np.float32, m_p60),
    'p70' : Metric('p70', np.float32, m_p70),
    'p75' : Metric('p75', np.float32, m_p75),
    'p80' : Metric('p80', np.float32, m_p80),
    'p90' : Metric('p90', np.float32, m_p90),
    'p95' : Metric('p95', np.float32, m_p95),
    'p99' : Metric('p99', np.float32, m_p99),
}