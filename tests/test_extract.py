import pytest
import os
from pathlib import Path
from osgeo import gdal
from pyproj import CRS

from silvimetric import shatter, extract
from silvimetric import Metrics, Attribute, ExtractConfig, Extents, Storage

@pytest.fixture(scope='function')
def tif_filepath(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("test_tifs")
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def extract_attrs(dims)->list[str]:
    yield [Attribute('Z', dtype=dims['Z']), Attribute('Intensity', dtype=dims['Intensity'])]

@pytest.fixture(scope='function')
def extract_config(tdb_filepath, tif_filepath, metrics, shatter_config, extract_attrs):
    shatter(shatter_config)
    yield ExtractConfig(tdb_filepath, tif_filepath, extract_attrs, metrics)

def tif_test(extract_config):
    minx, miny, maxx, maxy = extract_config.bounds.get()
    resolution = extract_config.resolution
    filenames = [Metrics[m.name].entry_name(a.name)
                    for m in extract_config.metrics
                    for a in extract_config.attrs]
    e = Extents.from_storage(Storage.from_db(extract_config.tdb_dir))
    yimin = e.indices['y'].min()

    for f in filenames:
        path = Path(extract_config.out_dir) / f'{f}.tif'
        assert path.exists()

        raster: gdal.Dataset = gdal.Open(str(path))
        derived = CRS.from_user_input(raster.GetProjection())
        assert derived == extract_config.crs

        xsize = int((maxx - minx) / resolution)
        ysize = int((maxy - miny) / resolution)
        assert raster.RasterXSize == xsize
        assert raster.RasterYSize == ysize

        r = raster.ReadAsArray()
        assert all([ r[y,x] == ((maxy/resolution)-(y+yimin))  for y in range(ysize) for x in range(xsize)])

class Test_Extract(object):

    def test_config(self, extract_config, tdb_filepath, tif_filepath, extract_attrs):
        assert all([a in [*extract_attrs, 'count'] for a in extract_config.attrs])
        assert all([a in extract_config.attrs for a in extract_attrs])
        assert extract_config.tdb_dir == tdb_filepath
        assert extract_config.out_dir == tif_filepath

    def test_extract(self, extract_config):
        extract(extract_config)
        tif_test(extract_config)

    def test_sub_bounds_extract(self, extract_config, storage):
        s = extract_config
        e = Extents.from_storage(storage)

        for b in e.split():
            ec = ExtractConfig(s.tdb_dir, s.out_dir, s.attrs,
                               s.metrics, b.bounds)
            extract(ec)
            tif_test(ec)
