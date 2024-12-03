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

@pytest.fixture(scope="function")
def process_dask() -> Generator[None, None, None]:
    dask.config.set(scheduler="processes")

    yield None
    dask.config.set(scheduler="single-threaded")

@pytest.fixture(scope='function')
def dask_proc_client(process_dask) -> Generator[None, None, None]:
    client = dask.distributed.Client()
    dask.config.set({'distributed.client': client})

    yield None
    dask.config.set({'distributed.client': None})
    client.close()

@pytest.fixture(scope='function')
def dask_thread_client(threaded_dask) -> Generator[None, None, None]:
    client = dask.distributed.Client()
    dask.config.set({'distributed.client': client})

    yield None
    dask.config.set({'distributed.client': None})
    client.close()