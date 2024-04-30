import pytest
from typing import Generator
from click.testing import CliRunner

from silvimetric.commands import shatter

@pytest.fixture(scope='session')
def runner():
    return CliRunner()

@pytest.fixture
def pre_shatter(shatter_config) -> Generator[int, None, None]:
    shatter_config.tile_size=1
    shatter_config.mbr = (((0,4), (0,4)),)
    yield shatter.shatter(shatter_config)