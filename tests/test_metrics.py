import numpy as np
import pandas as pd

from silvimetric import shatter, Storage, Metric
from silvimetric import all_metrics as s
from silvimetric import l_moments
from silvimetric.resources.attribute import Attribute
from silvimetric.resources.taskgraph import Graph
from silvimetric.resources.attribute import Pdal_Attributes as dims

from test_shatter import confirm_one_entry


class TestMetrics:
    def test_dag(
        self, metric_data: pd.DataFrame, metric_dag_results: pd.DataFrame
    ):
        metrics = list(l_moments.values())
        g = Graph(metrics).init()
        assert g.initialized
        assert all(n.initialized for n in g.nodes.values())
        assert not set(g.nodes.keys()) ^ set([
            'l1',
            'l2',
            'l3',
            'l4',
            'lcv',
            'lskewness',
            'lkurtosis',
            'lmombase',
        ])

        g.run(metric_data)
        for node in g.nodes.values():
            assert isinstance(node.results, pd.DataFrame)
            assert all(node.results.any())

        assert all(g.results == metric_dag_results)

    def test_metrics(
        self,
        metric_data: pd.DataFrame,
        metric_data_results: pd.DataFrame,
    ):
        # TODO fix this, changed to hilbert ordering and changed
        # to not using sort in groupbys
        ms = list(s.values())
        graph = Graph(ms)
        metrics = graph.run(metric_data)
        assert isinstance(metrics, pd.DataFrame)
        # cannot use pandas compare because dataframes may not have identical
        # column ordering, so compare values of each column
        for m in metric_data_results.columns:
            assert all(metric_data_results[m] == metrics[m])

    def test_dependencies(self, metric_data: pd.DataFrame):
        # should be able to create a dependency graph
        cv = s['cv']
        mean = s['mean']
        stddev = s['stddev']

        cv.dependencies = [mean, stddev]

        b = Graph([cv]).run(metric_data)
        # cv/mean should be there
        assert b.m_Z_cv.any()

        # and median/stddev should not
        assert not any(x in b.dtypes for x in ['m_Z_median', 'm_Z_stddev'])

    def test_filter(
        self, metric_shatter_config: pd.Series, test_point_count: int
    ):
        pc = shatter(metric_shatter_config)
        assert pc == test_point_count

        s = Storage.from_db(metric_shatter_config.tdb_dir)
        m = s.config.metrics[0]
        assert len(m.filters) == 1

        base = 11 if s.config.alignment == 'AlignToCenter' else 10
        maxy = s.config.root.maxy
        confirm_one_entry(s, maxy, base, test_point_count)

    def test_custom(
        self, metric_data: pd.DataFrame, attrs: list[Attribute]
    ) -> None:
        def m_over500(data):
            return data[data >= 500].count()

        z_att = attrs[0]
        m_cust = Metric(
            name='over500',
            dtype=np.float32,
            method=m_over500,
            attributes=[z_att],
        )

        b = Graph(m_cust).init().run(metric_data)

        assert b.m_Z_over500.any()
        assert b.m_Z_over500.values[0] == 2

    def test_dependency_passing(
        self, dep_crr: Metric, depless_crr: Metric, metric_data: pd.DataFrame
    ):
        nd1 = Graph(depless_crr).init().run(metric_data)
        nd2 = Graph(dep_crr).init().run(metric_data)
        assert all(nd2.m_Z_deps_crr == nd1.m_Z_depless_crr)
