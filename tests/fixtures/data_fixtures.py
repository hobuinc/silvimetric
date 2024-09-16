import pytest
import os
from typing import Generator

@pytest.fixture(scope='session')
def no_cell_line_pc() -> Generator[int, None, None]:
    yield 84100

@pytest.fixture(scope='session')
def no_cell_line_path() -> Generator[str, None, None]:
    path = os.path.join(os.path.dirname(__file__), "..", "data",
            "test_data_2.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='session')
def no_cell_line_pipeline() -> Generator[str, None, None]:
    path = os.path.join(os.path.dirname(__file__), "..", "data",
            "test_data_2.json")
    assert os.path.exists(path)
    yield os.path.abspath(path)

@pytest.fixture(scope='session')
def autzen_filepath() -> Generator[str, None, None]:
    path = os.path.join(os.path.dirname(__file__), "..", "data",
            "autzen-small.copc.laz")
    assert os.path.exists(path)
    yield os.path.abspath(path)