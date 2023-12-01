import tiledb
import numpy as np

from silvimetric import Storage
from silvimetric.metric import Metrics
from silvimetric import __version__ as svversion

class Test_Storage(object):

    def test_schema(self, storage: Storage, attrs: list[str], dims):
        with storage.open('r') as st:
            s:tiledb.ArraySchema = st.schema
            assert s.has_attr('count')
            assert s.attr('count').dtype == np.int32

            for a in attrs:
                assert s.has_attr(a)
                assert s.attr(a).dtype == dims[a]

    def test_local(self, storage: Storage, attrs: list[str], dims):
        with storage.open('r') as st:
            sc = st.schema
            assert sc.has_attr('count')
            assert sc.attr('count').dtype == np.int32

            for a in attrs:
                assert sc.has_attr(a)
                assert sc.attr(a).dtype == dims[a]

    def test_config(self, storage: Storage):
        """Check that instantiation metadata is properly written"""

        storage.saveConfig()
        config = storage.getConfig()
        assert config.resolution == storage.config.resolution
        assert config.bounds == storage.config.bounds
        assert config.crs == storage.config.crs
        assert storage.config.version == svversion

    def test_metrics(self, storage: Storage):
        m_list = storage.getMetrics()
        a_list = storage.getAttributes()

        with storage.open('r') as a:
            s: tiledb.ArraySchema = a.schema
            for m in m_list:
                assert m in Metrics.keys()
                assert all([s.attr(f'm_{att}_{m}').dtype == Metrics[m].dtype
                            for att in a_list])
