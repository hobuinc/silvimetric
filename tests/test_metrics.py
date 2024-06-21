import numpy as np
import pandas as pd

from silvimetric import shatter, Storage, grid_metrics, Metric
from silvimetric.resources.metrics import all_metrics as s

class TestMetrics():

    def test_metrics(self, metric_data):
        for mname in grid_metrics:
            m: Metric = grid_metrics[mname]
            d = m.do(metric_data)
            assert isinstance(d, pd.DataFrame), f"Metric {mname} failed to make a dataframe. Data created: {d}"

    def test_dependencies(self, metric_data):
        from dask import get

        cv = s['cv']
        mean = s['mean']
        stddev = s['stddev']
        median = s['median']

        mean.dependencies = [ median ]
        stddev.dependencies = [ median ]
        cv.dependencies = [ mean, stddev ]

        graph = cv.make_graph(metric_data)

        res = get(graph, 'cv')
        print(res)

    def test_filter(self, metric_shatter_config, test_point_count, maxy,
            resolution):

        m = metric_shatter_config.metrics[0]
        assert len(m.filters) == 1

        pc = shatter(metric_shatter_config)
        assert pc == test_point_count

        s = Storage.from_db(metric_shatter_config.tdb_dir)
        with s.open('r') as a:
            q = a.query(coords=False, use_arrow=False).df

            nor_mean = q[:]['m_NumberOfReturns_mean']
            nor = q[:]['NumberOfReturns']
            assert not nor_mean.isna().any()
            assert nor.notna().any()

            assert a[:,:]['Z'].shape[0] == 100
            xdom = a.schema.domain.dim('X').domain[1]
            ydom = a.schema.domain.dim('Y').domain[1]
            assert xdom == 10
            assert ydom == 10

            data = a.query(attrs=['Z'], coords=True, use_arrow=False).df[:]
            data = data.set_index(['X','Y'])

            for xi in range(xdom):
                for yi in range(ydom):
                    curr = data.loc[xi,yi]
                    curr.size == 1
                    curr.iloc[0].size == 900
                    # this should have all indices from 0 to 9 filled.
                    # if oob error, it's not this test's fault
                    assert bool(np.all( curr.iloc[0] == ((maxy/resolution) - (yi + 1)) ))

