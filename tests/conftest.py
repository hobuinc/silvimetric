import pytest
import os
import dask

@pytest.fixture(scope="session")
def autzen_classified() -> str:
    path = os.path.join(
            os.path.dirname(__file__),
            "data",
            "autzen_test.copc.laz"
    )
    assert os.path.exists(path)
    return os.path.abspath(path)

@pytest.fixture(scope="session", autouse=True)
def configure_dask():
    dask.config.set(scheduler="single-threaded")
