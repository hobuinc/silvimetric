from typing import Dict, Literal, Any, Self, Union
from uuid import uuid4

import dask
from dask.delayed import Delayed
from dask.highlevelgraph import HighLevelGraph
import dask.multiprocessing
import dask.threaded
import distributed
import dask.bag as db

from dask.optimization import cull

import pandas as pd
import base64
import dill

from .attribute import Attribute
from .metric import MetricFn, Metric

type MetricGraphDict = Dict[str, tuple[MetricFn, Literal['data'], Any]]

class MetricGraph():
    def __init__(self, graph: MetricGraphDict={}, deps: dict[str, list[str]]={}):
        self.graph: MetricGraphDict = graph
        self.deps = deps

    def add(self, metric) -> None:
        if isinstance(metric, Attribute):
            return
        self.graph[metric.name] = (metric.do, 'data',
                *(d.name for d in metric.dependencies) )
        for d in metric.dependencies:
            self.add(d)

    @property
    def hlg(self) -> HighLevelGraph:
        return HighLevelGraph(self.graph, self.deps)

    def get_runner(self):
        try:
            c= distributed.get_client()
        except:
            c = None

        if c is not None:
            # return dask.threaded.get
            return c.get
        else:
            s = dask.config.get('scheduler')
            if s == 'threads':
                return dask.threaded.get
            elif s == 'processes':
                return dask.multiprocessing.get
            elif s == 'single-threaded':
                return dask.get
            else:
                raise ValueError(f"Invalid dask scheduler, {s}")

    # def run(self, data: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    #     getter = self.get_runner()

    #     #make distinct graph for multiprocessing purposes
    #     u = uuid4()
    #     uks = [ f'{k}-{u}' for k in keys ]
    #     uk2 = [ (f'{k}',f'{u}') for k in keys ]
    #     g = {
    #             f'{k}-{u}': ( v[0], *( f'{vd}-{u}' for vd in v[1:] ) )
    #             for k,v in self.graph.items()
    #         } | { f'data-{u}': data }

    #     g2 = {
    #             (f'{k}', f'{u}'): ( v[0], *( (f'{vd}', f'{u}') for vd in v[1:] ) )
    #             for k,v in self.graph.items()
    #         } | { (f'data', f'{u}'): data }
    #     hlg = HighLevelGraph(*cull(g2, uk2))
    #     asdf = db.Bag(name=str(u), dsk=hlg, npartitions=5)

    #     metric_data : tuple[pd.DataFrame] = getter(g, uks)
    #     result: pd.DataFrame = metric_data[0]

    #     for m in metric_data[1:]:
    #         result = result.merge(m, left_index=True, right_index=True)

    #     return result

    def run(self, data: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        u = uuid4()
        ks = [ (f'{k}',f'{u}') for k in keys ]
        graph = {
               (k, u): [ v[0], *( (f'{vd}', f'{u}') for vd in v[1:] ) ]
               for k,v in self.graph.items()
           } | { ('data', f'{u}'): data }
        hlg = HighLevelGraph(*cull(graph, ks))
        hlg.validate()
        bag = db.Bag(dsk=hlg, name=str(u), npartitions=len(graph.keys()))
        return bag.persist()


    @staticmethod
    def get_methods(data, metrics: Union[Metric, list[Metric]], uuid=None):

        # identitity for this graph, can be created before or during this method
        # call, but needs to be the same across this graph, and unique compared
        # to other graphs
        if uuid is None:
            uuid = uuid4()

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
            ddeps = MetricGraph.get_methods(data, m.dependencies, uuid)
            dd = dask.delayed(m.do)(data, *ddeps,
                dask_key_name=f'{m.name}-{str(uuid)}')
            seq.append(dd)

        return seq

    @staticmethod
    def run_metrics(data, metrics: Union[Metric, list[Metric]]) -> pd.DataFrame:
        from functools import reduce

        graph = MetricGraph.get_methods(data, metrics)

        computed_list = dask.compute(*graph)


        def merge(x, y):
            return x.merge(y, on=['xi','yi'])
        merged = reduce(merge, computed_list)

        return merged

    @staticmethod
    def make_graph(metrics: Union[Metric, list[Metric]]) -> Self:
        if isinstance(metrics, Metric):
            metrics = [ metrics ]

        mg = MetricGraph()
        for m in metrics:
            mg.add(m)

        mg1, deps = cull(mg.graph, [m.name for m in metrics])

        return MetricGraph(mg1, deps)

    def to_json(self):
        return {
            f'{key}': (
                base64.b64encode(dill.dumps(val[0])).decode(),
                *val[1:]
            ) for key,val in self.graph.items()
        }

    @staticmethod
    def from_dict(d):
        graph = { key: (
                dill.loads(base64.b64decode(val[0].encode())),
                *val[1:]
            ) for key, val in d.items() }
        return MetricGraph(graph)