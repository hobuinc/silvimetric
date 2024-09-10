import json
import base64

from typing import Callable, Optional, Any, Union, List, Self
from inspect import getsource
from uuid import uuid4
from functools import reduce

from tiledb import Attr
import numpy as np
import distributed
import dill
import pandas as pd

import dask
from dask.delayed import Delayed
from distributed.client import _get_global_client as get_client
from distributed import Future



from .attribute import Attribute




MetricFn = Callable[[pd.DataFrame, Any], pd.DataFrame]
FilterFn = Callable[[pd.DataFrame, Optional[Union[Any, None]]], pd.DataFrame]

# Derived information about a cell of points
## TODO should create list of metrics as classes that derive from Metric?
class Metric():
    """
    A Metric is a TileDB entry representing derived cell data. There is a base set of
    metrics available through Silvimetric, or you can create your own. A Metric
    object has all the information necessary to facilitate the derivation of
    data as well as its insertion into the database.
    """
    def __init__(self, name: str, dtype: np.dtype, method: MetricFn,
            dependencies: list[Self]=[], filters: List[FilterFn]=[],
            attributes: List[Attribute]=[]) -> None:

        #TODO make deps, filters, attrs into tuples or sets, not lists so they're hashable

        self.name = name
        """Metric name. eg. mean"""
        self.dtype = np.dtype(dtype).str
        """Numpy data type."""
        self.dependencies = dependencies
        """Metrics this is dependent on."""
        self._method = method
        """The method that processes this data."""
        self.filters = filters
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
            'filters', frozenset(base64.b64encode(dill.dumps(f)).decode() for f in self.filters),
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

    def sanitize_and_run(self, d, *args):
        # args come in as a value wrapped in one index of a 2D dataframe
        # we need to remove the wrapping and pass the values to the method
        # to make things easier
        a = tuple( a.values[0][0] for a in args)
        return self._method(d, *a)

    def do(self, data: pd.DataFrame, *args) -> pd.DataFrame:
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

        data = self.run_filters(data)
        gb = data.groupby(idx)

        # create map of current column name to tuple of new column name and metric method
        cols = data.columns
        runner = lambda d: self.sanitize_and_run(d, *args)

        new_cols = {
            c: [ (self.entry_name(c), runner) ]
            for c in cols if c not in idx
        }

        val = gb.aggregate(new_cols, args=args)

        #remove hierarchical columns
        val.columns = val.columns.droplevel(0)
        return val

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
            'method': base64.b64encode(dill.dumps(self._method)).decode(),
            'filters': [base64.b64encode(dill.dumps(f)).decode() for f in self.filters],
            'attributes': [a.to_json() for a in self.attributes]
        }

    @staticmethod
    def from_dict(data: dict) -> "Metric":
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

def get_methods(data: pd.DataFrame | Delayed, metrics: Metric | list[Metric],
        uuid=None) -> list[Delayed]:
    """
    Create Metric dependency graph by iterating through desired metrics and
    their dependencies, creating Delayed objects that can be run later.
    """
    # identitity for this graph, can be created before or during this method
    # call, but needs to be the same across this graph, and unique compared
    # to other graphs
    if uuid is None:
        uuid = uuid4()

    # don't duplicate a delayed object
    if not isinstance(data, Delayed):
        data = dask.delayed(data)

    if isinstance(metrics, Metric):
        metrics = [ metrics ]

    # iterate through metrics and their dependencies.
    # uuid here will help guide metrics to use the same dependency method
    # calls from dask
    seq = []
    for m in metrics:
        if not isinstance(m, Metric):
            continue
        ddeps = get_methods(data, m.dependencies, uuid)
        dd = dask.delayed(m.do)(data, *ddeps,
            dask_key_name=f'{m.name}-{str(uuid)}')
        seq.append(dd)

    return seq

def run_metrics(data: pd.DataFrame, metrics: Union[Metric, list[Metric]]) -> pd.DataFrame:
    """
    Collect Metric dependency graph and run it, then merge the results together.
    """
    graph = get_methods(data, metrics)

    # try returning just the graph and see if that can speed thigns up
    # return graph

    computed_list = dask.persist(*graph, optimize_graph=True)

    def merge(x, y):
        return x.merge(y, on=['xi','yi'])
    merged = reduce(merge, computed_list)

    return merged