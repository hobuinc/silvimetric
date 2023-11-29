import numpy as np
from typing import Callable, Optional, Any, Union
from scipy import stats

StatFn = Callable[[np.ndarray, Optional[Union[Any, None]]], np.ndarray]

class Metric:
    def __init__(self, name: str, dtype: np.dtype, method: StatFn):
        self.name = name
        self._method = method
        self.dtype = dtype

    def att(self, attr: str):
        return f'm_{attr}_{self.name}'

    def do(self, data):
        return self._method(data)

    def __call__(self, data):
        return self._method(data)

Metrics = {
    'mean' : Metric('mean', np.float64, lambda data: np.mean(data, keepdims=True)),
    'mode' : Metric('mode', np.float64, lambda data: stats.mode(data, keepdims=True).mode),
    'median' : Metric('median', np.float64, lambda data: np.median(data, keepdims=True)),
    'min' : Metric('min', np.float64, lambda data: np.min(data, keepdims=True)),
    'max' : Metric('max', np.float64, lambda data: np.max(data, keepdims=True)),
    #TODO add all metrics from https://github.com/hobuinc/silvimetric/issues/5
    # 'stddev' : Metric('stddev', lambda data: np.std(data, keepdims=True)),
    # 'p01' : Metric('stddev', lambda data: np.std(data, keepdims=True)),
}