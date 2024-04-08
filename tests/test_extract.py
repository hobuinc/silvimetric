import pytest
import os
from pathlib import Path
from osgeo import gdal
from pyproj import CRS
import copy
import uuid
from typing import Generator

from silvimetric.commands.shatter import shatter
from silvimetric.commands.extract import extract
from silvimetric.resources import Metrics, Attribute, ExtractConfig, Extents, Log

@pytest.fixture(scope='function')
def tif_filepath(tmp_path_factory) -> Generator[str, None, None]:
    path = tmp_path_factory.mktemp("test_tifs")
    yield os.path.abspath(path)

@pytest.fixture(scope='function')
def extract_attrs(dims)->Generator[list[str], None, None]:
    yield [Attribute('Z', dtype=dims['Z']), Attribute('Intensity', dtype=dims['Intensity'])]

@pytest.fixture(scope='function')
def extract_config(tdb_filepath, tif_filepath, metrics, shatter_config, extract_attrs):
    shatter(shatter_config)
    log = Log(20)
    c =  ExtractConfig(tdb_dir = tdb_filepath,
                       log = log,
                       out_dir = tif_filepath,
                       attrs = extract_attrs,
                       metrics = metrics)
    yield c

@pytest.fixture(scope='function')
def multivalue_config(tdb_filepath, tif_filepath, metrics, shatter_config, extract_attrs):

    shatter(shatter_config)
    e = Extents.from_storage(shatter_config.tdb_dir)
    b: Extents = e.split()[0]

    second_config = copy.deepcopy(shatter_config)
    second_config.bounds = b.bounds
    second_config.name = uuid.uuid4()
    second_config.point_count = 0

    shatter(second_config)
    log = Log(20)
    c =  ExtractConfig(tdb_dir = tdb_filepath,
                       log = log,
                       out_dir = tif_filepath,
                       attrs = extract_attrs,
                       metrics = metrics)
    yield c

def tif_test(extract_config):
    minx, miny, maxx, maxy = extract_config.bounds.get()
    resolution = extract_config.resolution
    filenames = [Metrics[m.name].entry_name(a.name)
                    for m in extract_config.metrics
                    for a in extract_config.attrs]
    e = Extents.from_storage(extract_config.tdb_dir)
    yimin = e.get_indices()['y'].min()

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

    def test_sub_bounds_extract(self, extract_config):
        s = extract_config
        e = Extents.from_storage(s.tdb_dir)
        log = Log(20)

        for b in e.split():
            ec = ExtractConfig(tdb_dir = s.tdb_dir,
                               log = log,
                               out_dir = s.out_dir,
                               attrs = s.attrs,
                               metrics = s.metrics,
                               bounds = b.bounds)
            extract(ec)
            tif_test(ec)

    def test_multi_value(self, multivalue_config):
        extract(multivalue_config)
        tif_test(multivalue_config)

    def test_cli_extract(self, runner, extract_config):
        from silvimetric.cli import cli

        atts = ' '.join([f'-a {a.name}' for a in extract_config.attrs])
        ms = ' '.join([f'-m {m.name}' for m in extract_config.metrics])
        out_dir = extract_config.out_dir
        tdb_dir = extract_config.tdb_dir

        res = runner.invoke(cli.cli, args=(f'-d {tdb_dir} --scheduler '+
            f'single-threaded extract {atts} {ms} --outdir {out_dir}'))

        assert res.exit_code == 0

        tif_test(extract_config)