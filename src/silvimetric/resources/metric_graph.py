from typing import Dict, Literal, Any, Self, Union

from dask import get
import pandas as pd
import base64
import dill

from .attribute import Attribute
from .metric import MetricFn, Metric

type MetricGraphDict = Dict[str, tuple[MetricFn, Literal['data'], Any]]

class MetricGraph():
    def __init__(self, graph: MetricGraphDict):
        self.graph: MetricGraphDict = graph

    def add(self, metric) -> None:
        if isinstance(metric, Attribute):
            return
        self.graph[metric.name] = (metric.do, 'data',
                *(d.name for d in metric.dependencies) )
        for d in metric.dependencies:
            self.add(d)

    def run(self, data: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        self.graph['data'] = data
        metric_data : tuple[pd.DataFrame] = get(self.graph, keys)
        result: pd.DataFrame = metric_data[0]

        for m in metric_data[1:]:
            result = result.merge(m, left_index=True, right_index=True)
        return result

    @staticmethod
    def make_graph(metrics: Union[Metric, list[Metric]]) -> Self:
        if isinstance(metrics, Metric):
            metrics = [ metrics ]

        mg = MetricGraph({})
        for m in metrics:
            mg.add(m)

        return mg

    def to_json(self):
        return {
            f'{key}': (
                base64.b64encode(dill.dumps(val[0])).decode(),
                val[1:]
            ) for key,val in self.graph.items()
        }

    @staticmethod
    def from_dict(d):
        graph = { key: (
                dill.loads(base64.b64decode(val[0].encode())),
                val[1:]
            ) for key, val in d.items() }
        return MetricGraph(graph)