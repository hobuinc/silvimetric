import pytest
import os
from typing_extensions import Generator
import dask

@pytest.fixture(scope="session", autouse=True)
def configure_dask() -> None:
    dask.config.set(scheduler="single-threaded")

@pytest.fixture(scope="function")
def threaded_dask() -> Generator[None, None, None]:
    dask.config.set(scheduler="threads")

    yield None
    dask.config.set(scheduler="single-threaded")
