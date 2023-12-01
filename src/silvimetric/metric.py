import numpy as np
from typing import Callable, Optional, Any, Union, Self
from scipy import stats
from inspect import getsource
from abc import ABC, abstractmethod
from tiledb import Attr

from .entry import Entry

MetricFn = Callable[[np.ndarray, Optional[Union[Any, None]]], np.ndarray]

# Derived information about a cell of points

## TODO should create list of metrics as classes that derive from Metric?
class Metric(Entry):
    def __init__(self, name: str, dtype: np.dtype, deps: list[Entry], method: MetricFn):
        super(Entry, self).__init__(name, dtype, deps)
        self._method = method

    def schema(self, attr: str):
        entry_name = self.entry_name(attr)
        return Attr(name=entry_name, dtype=self._dtype)

    # common name, storage name
    def entry_name(self, attr: str) -> str:
        return f'm_{attr}_{self.name}'

    def do(self, data: np.ndarray) -> np.ndarray:
        return self._method(data)

    def __call__(self, data: np.ndarray) -> np.ndarray:
        return self._method(data)

    def __repr__(self) -> str:
        return getsource(self._method)

#TODO add all metrics from https://github.com/hobuinc/silvimetric/issues/5
Metrics = {
    'mean' : Metric('mean', np.float64, lambda data: np.mean(data)),
    'mode' : Metric('mode', np.float64, lambda data: stats.mode(data).mode),
    'median' : Metric('median', np.float64, lambda data: np.median(data)),
    'min' : Metric('min', np.float64, lambda data: np.min(data)),
    'max' : Metric('max', np.float64, lambda data: np.max(data)),
    'stddev' : Metric('stddev', np.float64, lambda data: np.std(data)),
    # 'p01' : Metric('p01', np.float64, lambda data: np.std(data, keepdims=True)),
}