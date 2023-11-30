import pytest
import os
from pathlib import Path
from osgeo import gdal
from pyproj import CRS

from silvimetric.shatter import shatter, ShatterConfiguration
from silvimetric.storage import Storage, Configuration
from silvimetric.extract import extract, ExtractConfiguration
from silvimetric.metric import Metrics

@pytest.fixture(scope='class')
def tdb_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tdb")
    yield os.path.abspath(path)

@pytest.fixture(scope='class')
def tif_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tifs")
    yield os.path.abspath(path)

@pytest.fixture(scope='class')
def metrics():
    yield ['mean', 'median']

@pytest.fixture(scope='class')
def storage_config(tdb_filepath, bounds, resolution, crs, attrs):
    yield Configuration(tdb_filepath, bounds, resolution, crs, attrs,
                        version='test_version', name='test_db')

@pytest.fixture(scope='class')
def shatter_config(tdb_filepath, filepath, tile_size, storage_config):
    Storage.create(storage_config)
    yield ShatterConfiguration(tdb_filepath, filepath, tile_size, debug=True)


@pytest.fixture(scope='class')
def extract_config(tdb_filepath, tif_filepath, metrics, shatter_config):
    shatter(shatter_config)
    yield ExtractConfiguration(tdb_filepath, tif_filepath, attrs=['Z'],
                               metrics=metrics)

class Test_Extract(object):

    def test_config(self, extract_config, tdb_filepath, tif_filepath, attrs):
        assert all([a in [*attrs, 'count'] for a in extract_config.attrs])
        assert all([a in extract_config.attrs for a in attrs])
        assert extract_config.tdb_dir == tdb_filepath
        assert extract_config.out_dir == tif_filepath

    def test_extract(self, extract_config, resolution, miny, maxy, minx, maxx):
        extract(extract_config)
        filenames = [Metrics[m].att(a)
                     for m in extract_config.metrics
                     for a in extract_config.attrs]
        for f in filenames:
            path = Path(extract_config.out_dir) / f'{f}.tif'
            assert path.exists()

            raster: gdal.Dataset = gdal.Open(str(path))
            derived = CRS.from_user_input(raster.GetProjection())
            assert derived == extract_config.crs

            xsize = (maxx - minx) / resolution
            ysize = (maxy - miny) / resolution
            assert raster.RasterXSize == xsize
            assert raster.RasterYSize == ysize

            r = raster.ReadAsArray()
            assert all([ r[y,x] == ((maxy/resolution)-y)  for y in range(10) for x in range(10)])
