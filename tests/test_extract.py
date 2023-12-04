import pytest
import os
from pathlib import Path
from osgeo import gdal
from pyproj import CRS

from silvimetric.shatter import shatter
from silvimetric.extract import extract, ExtractConfig
from silvimetric.metric import Metrics

@pytest.fixture(scope='function')
def tif_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tifs")
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def extract_attrs()->list[str]:
    yield ['Z', 'Intensity']

@pytest.fixture(scope='function')
def extract_config(tdb_filepath, tif_filepath, metrics, shatter_config, extract_attrs):
    shatter(shatter_config)
    yield ExtractConfig(tdb_filepath, tif_filepath, extract_attrs, metrics)

class Test_Extract(object):

    def test_config(self, extract_config, tdb_filepath, tif_filepath, extract_attrs):
        assert all([a in [*extract_attrs, 'count'] for a in extract_config.attrs])
        assert all([a in extract_config.attrs for a in extract_attrs])
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
            assert raster.RasterXSize == xsize + 1
            assert raster.RasterYSize == ysize + 1

            r = raster.ReadAsArray()
            assert all([ r[y,x] == ((maxy/resolution)-y)  for y in range(10) for x in range(10)])
