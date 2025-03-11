import json
import base64

from typing_extensions import Self, Callable, Any, Union, List
from functools import reduce
from threading import Lock

from tiledb import Attr
import numpy as np
import dill
import pandas as pd

from distributed import Future
from .attribute import Attribute
from .filter import Filter, FilterFn

MetricFn = Callable[[pd.DataFrame, Any], pd.DataFrame]
mutex = Lock()

class Metric():
    """
    A Metric is a TileDB entry representing derived cell data. There is a base set of
    metrics available through Silvimetric, or you can create your own. A Metric
    object has all the information necessary to facilitate the derivation of
    data as well as its insertion into the database.
    """
    def __init__(self, name: str, dtype: np.dtype, method: MetricFn,
            dependencies: list[Self]=[], filters: List[Union[FilterFn, Filter]]=[],
            attributes: List[Attribute]=[]) -> None:

        self.name = name
        """Metric name. Names should be unique across Metrics and Filters. eg. mean"""
        self.dtype = np.dtype(dtype).str
        """Numpy data type."""
        self.dependencies = dependencies
        """Metrics this is dependent on."""
        self._method = method
        """The method that processes this data."""
        fn = [
            f if isinstance(f, Filter) else Filter(method=f, name=f.__name__)
            for f in filters
        ]
        self.filters = fn
        """List of user-defined filters to perform before performing method."""
        self.attributes = attributes
        """List of Attributes this Metric applies to. If empty it's used for all
        Attributes"""

    def __eq__(self, other):
        if self.name != other.name:
            return False
        elif self.dtype != other.dtype:
            return False
        elif self.dependencies != other.dependencies:
            return False
        elif self._method != other._method:
            return False
        elif self.filters != other.filters:
            return False
        elif self.attributes != other.attributes:
            return False
        else:
            return True

    def __hash__(self):
        return hash((
            'name', self.name,
            'dtype', self.dtype,
            'dependencies', frozenset(self.dependencies),
            'method', base64.b64encode(dill.dumps(self._method)).decode(),
            'filters', frozenset(self.filters),
            'attrs', frozenset(self.attributes)))

    def schema(self, attr: Attribute) -> Any:
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

    def sanitize_and_run(self, d, **kwargs):
        """
        Sanitize kwargs. Results from Filters can be DataFrames, while
        values from Metrics will be an pandas aggregate. DataFrames are pared
        down to the index intersection with the incoming data and other
        values will be passed as they are.
        """
        pass_kwargs = {}
        if kwargs.items():
            with mutex:
                for k,v in kwargs.items():
                    if isinstance(v, pd.DataFrame):
                        pass_kwargs[k] = v.loc[v.index.intersection(d.index)]
                    else:
                        pass_kwargs[k] = v

        return self._method(d, **pass_kwargs)

    def do(self, data: pd.DataFrame, **deps) -> pd.DataFrame:
        """Run metric and filters. Use previously run metrics to avoid running
        the same thing multiple times."""

        # the index columns are determined by where this data is coming from
        # if it has xi and yi, then it's coming from shatter
        # if it has X and Y, then it's coming from extract as a rerun of a cell
        if isinstance(data, Future):
            data = data.result()

        idx = ['xi','yi']
        if any([i not in data.columns for i in idx]):
            idx = ['X','Y']

        if self.attributes:
            attrs = [*[a.name for a in self.attributes],*idx]
            data = data[attrs]

        # run metric filters over the data first
        idxer = data[idx]
        gb = data.groupby(idx)

        runner = lambda d: self.sanitize_and_run(d, **deps)

        # create map of current column name to tuple of new column name and metric method
        cols = data.columns
        prev_cols = [col for col in cols if col not in idx]
        new_cols = {
            c: [( self.entry_name(c), runner )]
            for c in prev_cols
        }

        val = gb.aggregate(new_cols)

        # remove hierarchical columns
        val.columns = val.columns.droplevel(0)
        return val

    #TODO make dict with key for each Attribute effected? {att: [fn]}
    # for now these filters apply to all Attributes
    def add_filter(self, fn: Filter | FilterFn, name: str=None):
        """
        Add filter method to list of filters to run before calling main method.
        """
        if isinstance(fn, Filter):
            self.filters.append(fn)
        else:
            self.filters.append(Filter(fn, name))

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
            'method': base64.b64encode(dill.dumps(self._method)).decode(),
            'filters': [base64.b64encode(dill.dumps(f)).decode() for f in self.filters],
            'attributes': [a.to_json() for a in self.attributes]
        }

    @staticmethod
    def from_dict(data: dict) -> Self:
        name = data['name']
        dtype = np.dtype(data['dtype'])
        method = dill.loads(base64.b64decode(data['method'].encode()))

        if 'dependencies' in data.keys() and \
                data['dependencies'] and \
                data['dependencies'] is not None:
            dependencies = [ Metric.from_dict(d) for d in data['dependencies'] ]
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
    def from_string(data: str) -> Self:
        j = json.loads(data)
        return Metric.from_dict(j)

    def __eq__(self, other) -> tuple:
        return (self.name == other.name and
                self.dtype == other.dtype and
                self.dependencies == other.dependencies and
                self._method == other._method,
                self.attributes == other.attributes,
                self.filters == other.filters)

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.do(data)

    def __repr__(self) -> str:
        return f"Metric_{self.name}"
