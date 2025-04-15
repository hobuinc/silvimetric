import os
import datetime
import json
import pytest

import pyproj
import rasterio
import pdal

import silvimetric as sm

@pytest.fixture(scope='function')
def fusion_data_path():
    yield os.path.join(os.path.dirname(__file__), "..", "data",
        "fusion_rasters", "all_cover_above2_30METERS.tif")

@pytest.fixture(scope='function')
def fusion_data(fusion_data_path):
    raster = rasterio.open(fusion_data_path)
    yield raster.read(1)

@pytest.fixture(scope='function')
def plumas_storage_config(tmp_path_factory):
    crs = pyproj.CRS.from_epsg(26910)
    bounds = sm.Bounds(minx=635547, maxx=635847, miny=4402305, maxy=4402805)
    gms = sm.grid_metrics.get_grid_metrics('Z', 2, 2).values()
    attr_names = ['Z', 'Intensity', 'NumberOfReturns', 'ReturnNumber']
    attrs = [a for k,a in sm.Attributes.items() if k in attr_names]
    pl_tdb_dir = tmp_path_factory.mktemp('plumas_tdb').as_posix()
    sc = sm.StorageConfig(root=bounds, crs=crs, metrics=gms, attrs=attrs,
        tdb_dir=pl_tdb_dir)
    sm.Storage.create(sc)
    yield sc

@pytest.fixture(scope='function')
def plumas_storage(plumas_storage_config):
    yield sm.Storage(plumas_storage_config)

@pytest.fixture(scope='function')
def plumas_data_path():
    pc_path = os.path.join(os.path.dirname(__file__), "..", "data",
        "NoCAL_PlumasNF_B2_2018_TestingData_FUSIONNormalized.copc.laz")

    yield pc_path

@pytest.fixture(scope='function')
def plumas_shatter_config(plumas_data_path, plumas_storage_config):
    yield sm.ShatterConfig(tdb_dir=plumas_storage_config.tdb_dir,
        filename=plumas_data_path, date=datetime.datetime.now())

@pytest.fixture(scope='function')
def plumas_tif_dir(tmp_path_factory):
    yield tmp_path_factory.mktemp('plumas_tifs').as_posix()

@pytest.fixture(scope='function')
def plumas_cover_file(plumas_tif_dir):
    yield os.path.join(plumas_tif_dir, 'm_Z_allcover.tif')

@pytest.fixture(scope='function')
def metric_map():
    yield dict()