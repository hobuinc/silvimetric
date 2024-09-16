import os
import pytest
from typing import Generator
import pandas as pd
import numpy as np

from silvimetric import Log, StorageConfig, ShatterConfig, Storage, Data, Bounds
from silvimetric import __version__ as svversion

@pytest.fixture(scope='function')
def autzen_storage(tmp_path_factory: pytest.TempPathFactory) -> Generator[StorageConfig, None, None]:
    path = tmp_path_factory.mktemp("test_tdb")
    p = os.path.abspath(path)

    srs = """PROJCS[\"NAD83 / Oregon GIC Lambert (ft)\",
GEOGCS[\"NAD83\",DATUM[\"North_American_Datum_1983\",SPHEROID[\"GRS 1980\",
6378137,298.257222101,AUTHORITY[\"EPSG\",\"7019\"]],AUTHORITY[\"EPSG\",\"6269\"]],
PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",
0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4269\"]],
PROJECTION[\"Lambert_Conformal_Conic_2SP\"],PARAMETER[\"latitude_of_origin\",
41.75],PARAMETER[\"central_meridian\",-120.5],PARAMETER[\"standard_parallel_1\",
43],PARAMETER[\"standard_parallel_2\",45.5],PARAMETER[\"false_easting\",
1312335.958],PARAMETER[\"false_northing\",0],UNIT[\"foot\",0.3048,
AUTHORITY[\"EPSG\",\"9002\"]],AXIS[\"Easting\",EAST],AXIS[\"Northing\",NORTH],
AUTHORITY[\"EPSG\",\"2992\"]]"""
    b = Bounds(635579.2,848884.83,639003.73,853536.21)
    sc = StorageConfig(b, srs, 10, tdb_dir=p)
    Storage.create(sc)
    yield sc

@pytest.fixture(scope='function')
def autzen_data(autzen_filepath: str, autzen_storage: StorageConfig) -> Generator[Data, None, None]:
    d = Data(autzen_filepath, autzen_storage)
    yield d

@pytest.fixture(scope='function')
def metric_data(autzen_data: Data) -> Generator[pd.DataFrame, None, None]:
    p = autzen_data.pipeline
    autzen_data.execute()
    points = p.get_dataframe(0)
    points.loc[:, 'xi'] = np.floor(points.xi)
    points.loc[:, 'yi'] = np.ceil(points.yi)
    points = points.loc[points.xi == 1]
    points = points.loc[points.yi == 437]
    yield points[['Z', 'xi', 'yi']]

@pytest.fixture(scope='function')
def metric_shatter_config(tmp_path_factory, copc_filepath, attrs, metrics, bounds,
        date, crs, resolution) -> Generator[pd.Series, None, None]:

    path = tmp_path_factory.mktemp("test_tdb")
    p = os.path.abspath(path)
    log = Log('DEBUG')

    def dummy_fn(df: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(df, pd.DataFrame)
        ndf = df[df['NumberOfReturns'] >= 1]
        assert isinstance(ndf, pd.DataFrame)
        return ndf

    metrics[0].add_filter(dummy_fn, 'This is a function.')
    metrics[0].attributes=attrs

    """Make output"""
    st_config=StorageConfig(tdb_dir=p,
                        log=log,
                        crs=crs,
                        root=bounds,
                        resolution=resolution,
                        attrs=attrs,
                        metrics=metrics,
                        version=svversion)

    s = Storage.create(st_config)
    sh_config = ShatterConfig(tdb_dir=p,
            log=log,
            filename=copc_filepath,
            bounds=bounds,
            debug=True,
            date=date)
    yield sh_config