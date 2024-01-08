import json
import numpy as np
from typing import Callable, Optional, Any, Union, Self
from scipy import stats
from inspect import getsource
from tiledb import Attr
import dask
from line_profiler import profile

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

    def schema(self, attr: str):
        entry_name = self.entry_name(attr)
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
            'method': getsource(self._method)
        }

    def from_string(data: Union[str, dict]):
        if isinstance(data, str):
            j = json.loads(data)
        elif isinstance(data, dict):
            j = data
        name = j['name']

        # TODO should this be derived from config or from list of metrics?
        # dtype = j['dtype']
        # deps = j['dependencies']
        # method = j['method']
        # method = Metrics[name].method

        return Metrics[name]

    def __eq__(self, other):
        return super().__eq__(other) and self._method == other._method

    def __call__(self, data: np.ndarray) -> np.ndarray:
        return self._method(data)

    def __repr__(self) -> str:
        return getsource(self._method)

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

Metrics = {
    'mean' : Metric('mean', np.float64, m_mean),
    # 'mode' : Metric('mode', np.float64, m_mode),
    'median' : Metric('median', np.float64, m_median),
    'min' : Metric('min', np.float64, m_min),
    'max' : Metric('max', np.float64, m_max),
    'stddev' : Metric('stddev', np.float64, m_stddev),
}