import json
import base64

from typing_extensions import (
    Self,
    Callable,
    Optional,
    Any,
    Union,
    List,
    Literal,
)
from functools import reduce
from threading import Lock

from tiledb import Attr, FilterList, ZstdFilter
import numpy as np
import dill
import pandas as pd

from distributed import Future
from .attribute import Attribute

MetricFn = Callable[[pd.DataFrame, Any], pd.DataFrame]
FilterFn = Callable[[pd.DataFrame, Optional[Union[Any, None]]], pd.DataFrame]
NanPolicy = Union[Literal['propagate'], Literal['raise']]

mutex = Lock()


class Metric:
    """
    A Metric is a TileDB entry representing derived cell data. There is a base
    set of metrics available through Silvimetric, or you can create your own.
    A Metric object has all the information necessary to facilitate the
    derivation of data as well as its insertion into the database.
    """

    def __init__(
        self,
        name: str,
        dtype: np.dtype,
        method: MetricFn,
        dependencies: Optional[list[Self]] = None,
        filters: Optional[List[FilterFn]] = None,
        attributes: Optional[List[Attribute]] = None,
        nan_policy: NanPolicy = 'propagate',
    ) -> None:
        self.name = name
        """Metric name. eg. mean"""
        self.dtype = np.dtype(dtype).str
        """Numpy data type."""
        if dependencies is not None:
            self.dependencies = dependencies
        else:
            self.dependencies = []
        """Metrics this is dependent on."""
        self._method = method
        """The method that processes this data."""
        if filters is not None:
            self.filters = filters
        else:
            self.filters = []
            """
            List of user-defined filters to perform before performing method.
            """
        if attributes is not None:
            self.attributes = attributes
        else:
            self.attributes = []
        """List of Attributes this Metric applies to. If empty it's used for all
        Attributes"""
        self.nan_policy = nan_policy
        """Describe how metric should handle NaN found in dependencies.

        - propagate: if a NaN is present in the dependencies, return NaN for
        this metric as well.
        - raise: if a NaN is present, a ValueError will be raised.
        """
        self.nan_value = -9999
        """ Value to denote empty space or invalid values. """
        kind = np.dtype(self.dtype).kind
        if kind in ['i', 'f']:
            self.nan_value = -9999
        elif kind == 'u':
            self.nan_value = 0
        elif kind == 'O':
            self.nan_value = []
        else:
            # uncertain if we want to limit to numbers, more convenient for now
            raise ValueError(
                f'Invalid metric data type "{kind}", must be a number or'
                ' object.'
            )

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
        return hash(
            (
                'name',
                self.name,
                'dtype',
                self.dtype,
                'dependencies',
                frozenset(self.dependencies),
                'method',
                base64.b64encode(dill.dumps(self._method)).decode(),
                'filters',
                frozenset(
                    base64.b64encode(dill.dumps(f)).decode()
                    for f in self.filters
                ),
                'attrs',
                frozenset(self.attributes),
            )
        )

    def schema(self, attr: Attribute) -> Any:
        """
        Create schema for TileDB creation.

        :param attr: :class:`silvimetric.resources.entry.Atttribute`
        :return: TileDB Attribute
        """
        entry_name = self.entry_name(attr.name)
        return Attr(
            name=entry_name,
            dtype=self.dtype,
            filters=FilterList([ZstdFilter()]),
            nullable=True
        )

    def entry_name(self, attr: str) -> str:
        """Name for use in TileDB and extract file generation."""
        return f'm_{attr}_{self.name}'

    def sanitize_and_run(self, d, locs, args):
        """Sanitize arguments, find the indices"""
        # Args are the return values of previous DataFrame aggregations.
        # In order to access the correct location, we need a map of groupby
        # indices to their locations and then grab the correct index from args

        attr = d.name
        attrs = [a.entry_name(attr) for a in self.dependencies]

        if isinstance(args, pd.DataFrame):
            with mutex:
                idx = locs.loc[d.index[0]]
                xi = idx.xi
                yi = idx.yi
                pass_args = []
                for a in attrs:
                    try:
                        arg = args.at[(yi, xi), a]
                        if isinstance(arg, list) or isinstance(arg, tuple):
                            pass_args.append(arg)
                        elif np.isnan(arg):
                            return self.nan_value
                        else:
                            pass_args.append(arg)

                    except Exception as e:
                        if self.nan_policy == 'propagate':
                            return self.nan_value
                        else:
                            raise (e)
        else:
            pass_args = args

        return self._method(d, *pass_args)

    def do(self, data: pd.DataFrame, *args) -> pd.DataFrame:
        """Run metric and filters. Use previously run metrics to avoid running
        the same thing multiple times."""

        # the index columns are determined by where this data is coming from
        # if it has xi and yi, then it's coming from shatter
        # if it has X and Y, then it's coming from extract as a rerun of a cell
        if isinstance(data, Future):
            data = data.result()

        idx = ['yi', 'xi']
        if any([i not in data.columns for i in idx]):
            idx = ['Y', 'X']

        # run metric filters over the data first
        data = self.run_filters(data)

        if self.attributes:
            attrs = [*[a.name for a in self.attributes], *idx]
            data = data[attrs]

        idxer = data[idx]
        gb = data.groupby(idx)

        # Arguments come in as separate dataframes returned from previous
        # metrics deemed dependencies. If there are dependencies for this
        # metric, we'll merge the outputs from those here so they're easier
        # to work with.
        def merge(left, right):
            return left.merge(right, on=idx)

        if len(args) > 1:
            merged_args = reduce(merge, args)
        elif len(args) == 1:
            merged_args = args[0]
        else:
            merged_args = args

        # lambda method for use in dataframe aggregator
        def runner(d):
            return self.sanitize_and_run(d, idxer, merged_args)

        # create map of current column name to tuple of new column name and
        # metric method
        cols = data.columns
        prev_cols = [col for col in cols if col not in idx]
        new_cols = {c: [(self.entry_name(c), runner)] for c in prev_cols}

        val = gb.aggregate(new_cols)

        # remove hierarchical columns
        val.columns = val.columns.droplevel(0)

        # set nans to values for datatype and return value as the dtype
        if np.dtype(self.dtype).kind not in ['f', 'O']:
            np.nan_to_num(
                val,
                nan=self.nan_value,
                posinf=self.nan_value,
                neginf=self.nan_value,
            )

        return val

    def add_filter(self, fn: FilterFn):
        """
        Add filter method to list of filters to run before calling main method.
        """
        self.filters.append(fn)

    def run_filters(self, data: pd.DataFrame) -> pd.DataFrame:
        for f in self.filters:
            ndf = f(data)
            # TODO should this check be here?
            if not isinstance(ndf, pd.DataFrame):
                raise TypeError(
                    'Filter outputs must be a DataFrame. '
                    f'Type detected: {type(ndf)}'
                )
            data = ndf
        return data

    def to_json(self) -> dict[str, any]:
        return {
            'name': self.name,
            'dtype': np.dtype(self.dtype).str,
            'dependencies': [d.to_json() for d in self.dependencies],
            'method': base64.b64encode(dill.dumps(self._method)).decode(),
            'filters': [
                base64.b64encode(dill.dumps(f)).decode() for f in self.filters
            ],
            'attributes': [a.to_json() for a in self.attributes],
        }

    @staticmethod
    def from_dict(data: dict) -> 'Metric':
        name = data['name']
        dtype = np.dtype(data['dtype'])
        method = dill.loads(base64.b64decode(data['method'].encode()))

        if (
            'dependencies' in data.keys()
            and data['dependencies']
            and data['dependencies'] is not None
        ):
            dependencies = [Metric.from_dict(d) for d in data['dependencies']]
        else:
            dependencies = []

        if (
            'attributes' in data.keys()
            and data['attributes']
            and data['attributes'] is not None
        ):
            attributes = [Attribute.from_dict(a) for a in data['attributes']]
        else:
            attributes = []

        if (
            'filters' in data.keys()
            and data['filters']
            and data['filters'] is not None
        ):
            filters = [dill.loads(base64.b64decode(f)) for f in data['filters']]
        else:
            filters = []

        return Metric(name, dtype, method, dependencies, filters, attributes)

    @staticmethod
    def from_string(data: str) -> Self:
        j = json.loads(data)
        return Metric.from_dict(j)

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.do(data)

    def __repr__(self) -> str:
        return f'Metric_{self.name}'
