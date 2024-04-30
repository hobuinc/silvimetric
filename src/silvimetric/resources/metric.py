import json
import numpy as np
from typing import Callable, Optional, Any, Union, List
from inspect import getsource
from tiledb import Attr
import dask
import base64
import dill
import pandas as pd

from .entry import Attribute, Entry, Attributes

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
        for fn in self.filters:
            data = fn(data)
        return self._method(data)

    @dask.delayed
    def do_delayed(self, data: pd.DataFrame) -> np.ndarray:
        """Run metric as a dask delayed method"""
        self.do(data)

    #TODO make dict with key for each Attribute effected? {att: [fn]}
    # for now these filters apply to all Attributes
    def add_filter(self, fn: FilterFn):
        """
        Add filter method to list of filters to run before calling main method.
        """
        self.filters.append(fn)

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
        dependencies = data['dependencies']
        method = dill.loads(base64.b64decode(data['method'].encode()))
        attributes = [ Attribute.from_dict(a) for a in data['attributes']]
        filters = dill.loads(base64.b64decode(data['filters']).encode())

        return Metric(name, dtype, method, dependencies, filters, attributes)

    @staticmethod
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

def f_numret2(data):
    return np.where(data['NumberOfReturns']) > 2

#TODO change to correct dtype
Metrics = {
    'mean' : Metric('mean', np.float32, m_mean),
    'mode' : Metric('mode', np.float32, m_mode),
    'median' : Metric('median', np.float32, m_median),
    'min' : Metric('min', np.float32, m_min),
    'max' : Metric('max', np.float32, m_max, [Attributes['NumberOfReturns']],
        filters=[f_numret2]),
    'stddev' : Metric('stddev', np.float32, m_stddev),
}