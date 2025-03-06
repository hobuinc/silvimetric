import json
import base64
from typing_extensions import Callable, Optional, Any, Union, Self

import pandas as pd
import dill

FilterFn = Callable[[pd.DataFrame, Optional[Union[Any, None]]], pd.DataFrame]

class Filter():
    """
    A Filter is a method wrapper class that allows the DAG to more easily
    determine which filters have already been run.
    """
    def __init__(self, method: FilterFn, name: str=None, dependencies: list[Union[FilterFn, Self]]=[]):
        if not isinstance(method, Callable):
            raise ValueError('Invalid method passed to Filter, {}')
        self._method = method
        """Method that filters this data."""
        if name is None:
            name = self._method.__name__
        self.name = name
        """Name of the Filter. Names should be unique across Metrics and Filters."""
        fn = [
            f if isinstance(f, Filter) else Filter(f.__name__, f)
            for f in dependencies
        ]
        self.dependencies = fn
        """List of Filter dependencies to run before this Filter is run."""

    def __eq__(self, other):
        if self.name != other.name:
            return False
        elif self._method != other._method:
            return False

    def __hash__(self):
        return hash((
            'name', self.name,
            'method', base64.b64encode(dill.dumps(self._method)).decode(),
        ))

    def __repr__(self):
        return f"Filter_{self.name}"

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.do(data)

    def do(self, data: pd.DataFrame, *args):
        return self._method(data, args)

    def to_json(self) -> dict[str, any]:
        return {
            'name': self.name,
            'method': base64.b64encode(dill.dumps(self._method)).decode(),
        }

    @staticmethod
    def from_dict(data: dict):
        name = data['name']
        method = dill.loads(base64.b64decode(data['method'].encode()))
        return Filter(name, method)

    @staticmethod
    def from_string(data: str):
        d = json.loads(data)
        return Filter.from_dict(d)
