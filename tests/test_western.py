import tiledb
import numpy as np
from silvimetric import Storage


class Test_Western(object):
    def test_schema(self, western_storage: Storage):
        with western_storage.open('r') as st:
            s: tiledb.ArraySchema = st.schema
            assert s.has_attr('count')
            assert s.attr('count').dtype == np.uint32
