import json
import numpy as np
from scipy import stats
from typing import Callable, Optional, Any, Union, List
from inspect import getsource
from tiledb import Attr
import dask
import base64
import dill
import pandas as pd

from .entry import Attribute, Entry, Attributes
from .lmom4 import lmom4

MetricFn = Callable[[np.ndarray], np.ndarray]
FilterFn = Callable[[np.ndarray, Optional[Union[Any, None]]], np.ndarray]

# Derived information about a cell of points
## TODO should create list of metrics as classes that derive from Metric?
class Metric(Entry):
    """
    A Metric is an Entry representing derived cell data. There is a base set of
    metrics available through Silvimetric, or you can create your own. A Metric
    object has all the information necessary to facilitate the derivation of
    data as well as its insertion into the database.
    """
    def __init__(self, name: str, dtype: np.dtype, method: MetricFn,
            dependencies: list[Entry]=[], filters: List[FilterFn]=[],
            attributes: List[Attribute]=[]):

        super().__init__()
        self.name = name
        """Metric name. eg. mean"""
        self.dtype = dtype
        """Numpy data type."""
        self.dependencies = dependencies
        """Attributes/Metrics this is dependent on."""
        self._method = method
        """The method that processes this data."""
        self.filters = filters
        """List of user-defined filters to perform before performing method."""
        self.attributes = attributes
        """List of Attributes this Metric applies to. If empty it's used for all
        Attributes"""

    def schema(self, attr: Attribute):
        """
        Create schema for TileDB creation.

        :param attr: :class:`silvimetric.resources.entry.Atttribute`
        :return: TileDB Attribute
        """
        entry_name = self.entry_name(attr.name)
        return Attr(name=entry_name, dtype=self.dtype)

    def entry_name(self, attr: str) -> str:
        """Name for use in TileDB and extract file generation."""
        return f'm_{attr}_{self.name}'

    def do(self, data: pd.DataFrame) -> pd.DataFrame:
        """Run metric and filters."""
        idx = ['xi','yi']
        if any([i not in data.columns for i in idx]):
            idx = ['X','Y']

        if self.attributes:
            attrs = [*[a.name for a in self.attributes],*idx]
            data = data[attrs]

        data = self.run_filters(data)
        gb = data.groupby(idx)

        # try:
        #     idx = ['xi','yi']
        #     gb = data.groupby(idx)
        # except Exception as e:
        #     # coming from extract re-run
        #     idx = ['X','Y']
        #     gb = data.groupby(idx)

        # create map of current column name to tuple of new column name and metric method
        cols = data.columns

        new_cols = {
            c: [(self.entry_name(c), self._method)]
            for c in cols if c not in idx
        }

        val = gb.agg(new_cols)

        #remove hierarchical columns
        val.columns = val.columns.droplevel(0)
        return val

    @dask.delayed
    def do_delayed(self, data: pd.DataFrame) -> pd.DataFrame:
        """Run metric as a dask delayed method"""
        return self.do(data)

    #TODO make dict with key for each Attribute effected? {att: [fn]}
    # for now these filters apply to all Attributes
    def add_filter(self, fn: FilterFn, desc: str):
        """
        Add filter method to list of filters to run before calling main method.
        """
        self.filters.append(fn)

    def run_filters(self, data: pd.DataFrame) -> pd.DataFrame:
        for f in self.filters:
            ndf = f(data)
            #TODO should this check be here?
            if not isinstance(ndf, pd.DataFrame):
                raise TypeError('Filter outputs must be a DataFrame. '
                        f'Type detected: {type(ndf)}')
            data = ndf
        return data


    def to_json(self) -> dict[str, any]:
        return {
            'name': self.name,
            'dtype': np.dtype(self.dtype).str,
            'dependencies': [d.to_json() for d in self.dependencies],
            'method_str': getsource(self._method),
            'method': base64.b64encode(dill.dumps(self._method)).decode(),
            'filters': [base64.b64encode(dill.dumps(f)).decode() for f in self.filters],
            'attributes': [a.to_json() for a in self.attributes]
        }

    @staticmethod
    def from_dict(data: dict):
        name = data['name']
        dtype = np.dtype(data['dtype'])
        method = dill.loads(base64.b64decode(data['method'].encode()))

        if 'dependencies' in data.keys() and \
                data['dependencies'] and \
                data['dependencies'] is not None:
            dependencies = [ Attribute.from_dict(d) for d in data['dependencies'] ]
        else:
            dependencies = [ ]

        if 'attributes' in data.keys() and \
                data['attributes'] and \
                data['attributes'] is not None:
            attributes = [ Attribute.from_dict(a) for a in data['attributes']]
        else:
            attributes = [ ]

        if 'filters' in data.keys() and \
                data['filters'] and \
                data['filters'] is not None:
            filters = [ dill.loads(base64.b64decode(f)) for f in data['filters'] ]
        else:
            filters = [ ]

        return Metric(name, dtype, method, dependencies, filters, attributes)

    @staticmethod
    def from_string(data: str):
        j = json.loads(data)
        return Metric.from_dict(j)

    def from_string(data: str):
        j = json.loads(data)
        return Metric.from_dict(j)

    def __eq__(self, other):
        return (self.name == other.name and
                self.dtype == other.dtype and
                self.dependencies == other.dependencies and
                self._method == other._method,
                self.attributes == other.attributes,
                self.filters == other.filters)

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.do(data)

    def __repr__(self) -> str:
        return json.dumps(self.to_json())

#TODO add all metrics from https://github.com/hobuinc/silvimetric/issues/5

def m_mean(data):
    return np.mean(data)

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
    def mode_center(data, axis):
        return stats.mode(data, axis=axis).mode
    return stats.median_abs_deviation(data, center=mode_center)

# TODO test various methods for interpolation=... for all percentile-related metrics
# I think the default matches FUSION method but need to test
def m_iq(data):
    return stats.iqr(data)

def m_90m10(data):
    p = np.percentile(data, [10,90])
    return p[1] - p[0]

def m_95m05(data):
    p = np.percentile(data, [5,95])
    return p[1] - p[0]

def m_crr(data):
    return (np.mean(data) - np.min(data)) / (np.max(data) - np.min(data))

def m_sqmean(data):
    return np.sqrt(np.mean(np.square(data)))

def m_cumean(data):
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
    return np.mean(data)

def m_l2(data):
    l = lmom4(data)
    return l[1]

def m_l3(data):
    l = lmom4(data)
    return l[2]

def m_l4(data):
    l = lmom4(data)
    return l[3]

def m_lcv(data):
    l = lmom4(data)
    try:
        return l[1] / l[0]
    except ZeroDivisionError as e:
        return np.nan

def m_lskewness(data):
    l = lmom4(data)
    try:
        return l[2] / l[1]
    except ZeroDivisionError as e:
        return np.nan

def m_lkurtosis(data):
    l = lmom4(data)
    try:
        return l[3] / l[1]
    except ZeroDivisionError as e:
        return np.nan

# not sure how an array of metrics can be ingested by shatter
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


# TODO example for cover using all returns and a height threshold
# the threshold must be a parameter and not hardcoded
def m_cover(data):
    threshold = 2
    return (data > threshold).sum() / len(data)

def f_2plus(data):
    return data[data['HeightAboveGround'] > 2]

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
    'crr' : Metric('crr', np.float32, m_crr),
    'sqmean' : Metric('sqmean', np.float32, m_sqmean),
    'cumean' : Metric('cumean', np.float32, m_cumean),
    'l1' : Metric('l1', np.float32, m_l1),
    'l2' : Metric('l2', np.float32, m_l2),
    'l3' : Metric('l3', np.float32, m_l3),
    'l4' : Metric('l4', np.float32, m_l4),
    'lcv' : Metric('iq', np.float32, m_lcv),
    'lskewness' : Metric('lskewness', np.float32, m_lskewness),
    'lkurtosis' : Metric('lkurtosis', np.float32, m_lkurtosis),
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
    'cover' : Metric('cover', np.float32, m_cover),
    'profilearea' : Metric('profilearea', np.float32, m_profilearea),
}