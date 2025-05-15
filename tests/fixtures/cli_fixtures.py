import pytest
from typing_extensions import Generator
from click.testing import CliRunner

from silvimetric.commands import shatter
from silvimetric.resources.config import ShatterConfig



@pytest.fixture(scope='session')
def runner():
    return CliRunner()


@pytest.fixture
def pre_shatter(shatter_config: ShatterConfig) -> Generator[int, None, None]:
    shatter_config.tile_size = 1
    shatter_config.mbr = (((0, 4), (0, 4)),)
    yield shatter.shatter(shatter_config)
