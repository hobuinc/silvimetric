import pytest
import tiledb

from treetally.storage import Storage

class Test_Storage(object):
    @pytest.fixture(scope="class")
    def schema() -> tiledb.ArraySchema:


    def test_schema()