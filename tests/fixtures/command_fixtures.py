import uuid
import pytest

from time import sleep
from typing_extensions import List, Generator
from datetime import datetime

from silvimetric.commands import shatter
from silvimetric import ShatterConfig

@pytest.fixture(scope='function')
def config_split(shatter_config: ShatterConfig) -> Generator[List[ShatterConfig], None, None]:
    sc = shatter_config
    config_split = []
    day = 1
    for b in sc.bounds.bisect():
        day += 1
        date = datetime(2011, 1, day)

        config_split.append(ShatterConfig(
            filename=sc.filename,
            date=date,
            attrs=sc.attrs,
            metrics=sc.metrics,
            bounds=b,
            name=uuid.uuid4(),
            tile_size=4,
            tdb_dir=sc.tdb_dir,
            log=sc.log
        ))

    for s in config_split:
        shatter.shatter(s)
    sleep(1)

    yield config_split