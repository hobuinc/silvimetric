import os
import datetime
import pytest

import pyproj
from osgeo import gdal
import numpy as np

import silvimetric as sm


@pytest.fixture(scope='function')
def fusion_tif_dir():
    yield os.path.join(
        os.path.dirname(__file__), '..', 'data', 'fusion_rasters'
    )


@pytest.fixture(scope='function')
def fusion_data_path():
    yield os.path.join(
        os.path.dirname(__file__),
        '..',
        'data',
        'fusion_rasters',
        'all_cover_above2_30METERS.tif',
    )


@pytest.fixture(scope='function')
def fusion_data(fusion_data_path: str):
    raster = gdal.Open(fusion_data_path)
    raster_data = np.array(raster.GetRasterBand(1).ReadAsArray())
    yield raster_data


@pytest.fixture(scope='function')
def plumas_storage_config(tmp_path_factory: pytest.TempPathFactory):
    crs = pyproj.CRS.from_epsg(26910)
    bounds = sm.Bounds(minx=635547, maxx=635847, miny=4402347.17, maxy=4402805)
    gms = sm.grid_metrics.get_grid_metrics('Z', 2, 2).values()
    attr_names = ['Z', 'Intensity', 'NumberOfReturns', 'ReturnNumber']
    attrs = [a for k, a in sm.Attributes.items() if k in attr_names]
    pl_tdb_dir = tmp_path_factory.mktemp('plumas_tdb').as_posix()
    sc = sm.StorageConfig(
        root=bounds, crs=crs, metrics=gms, attrs=attrs, tdb_dir=pl_tdb_dir
    )
    sm.Storage.create(sc)
    yield sc


@pytest.fixture(scope='function')
def plumas_storage(plumas_storage_config: sm.StorageConfig):
    yield sm.Storage(plumas_storage_config)


@pytest.fixture(scope='function')
def plumas_data_path():
    pc_path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'data',
        'NoCAL_PlumasNF_B2_2018_TestingData_FUSIONNormalized.copc.laz',
    )

    yield pc_path


@pytest.fixture(scope='function')
def plumas_shatter_config(
    plumas_data_path: str, plumas_storage_config: sm.StorageConfig
):
    yield sm.ShatterConfig(
        tdb_dir=plumas_storage_config.tdb_dir,
        filename=plumas_data_path,
        date=datetime.datetime.now(),
    )


@pytest.fixture(scope='function')
def plumas_tif_dir(tmp_path_factory: pytest.TempPathFactory):
    yield tmp_path_factory.mktemp('plumas_tifs').as_posix()


@pytest.fixture(scope='function')
def plumas_cover_file(plumas_tif_dir: str):
    yield os.path.join(plumas_tif_dir, 'm_Z_allcover.tif')


@pytest.fixture(scope='function')
def metric_map(plumas_tif_dir: str, fusion_tif_dir: str):
    mapping = {
        'elev_AAD_2plus_30METERS.tif': 'm_Z_aad.tif',
        'elev_CV_2plus_30METERS.tif': 'm_Z_cv.tif',
        'elev_IQ_2plus_30METERS.tif': 'm_Z_iq.tif',
        'elev_L1_2plus_30METERS.tif': 'm_Z_l1.tif',
        'elev_L2_2plus_30METERS.tif': 'm_Z_l2.tif',
        'elev_L3_plus_30METERS.tif': 'm_Z_l3.tif',
        'elev_L4_2plus_30METERS.tif': 'm_Z_l4.tif',
        'elev_LCV_2plus_30METERS.tif': 'm_Z_lcv.tif',
        'elev_Lkurtosis_2plus_30METERS.tif': 'm_Z_lkurtosis.tif',
        'elev_Lskewness_2plus_30METERS.tif': 'm_Z_lskewness.tif',
        'elev_MAD_median_30METERS.tif': 'm_Z_mad_median.tif',
        'elev_MAD_mode_30METERS.tif': 'm_Z_mad_mode.tif',
        'elev_P01_2plus_30METERS.tif': 'm_Z_p01.tif',
        'elev_P05_2plus_30METERS.tif': 'm_Z_p05.tif',
        'elev_P10_2plus_30METERS.tif': 'm_Z_p10.tif',
        'elev_P20_2plus_30METERS.tif': 'm_Z_p20.tif',
        'elev_P25_2plus_30METERS.tif': 'm_Z_p25.tif',
        'elev_P30_2plus_30METERS.tif': 'm_Z_p30.tif',
        'elev_P40_2plus_30METERS.tif': 'm_Z_p40.tif',
        'elev_P50_2plus_30METERS.tif': 'm_Z_p50.tif',
        'elev_P60_2plus_30METERS.tif': 'm_Z_p60.tif',
        'elev_P70_2plus_30METERS.tif': 'm_Z_p70.tif',
        'elev_P75_2plus_30METERS.tif': 'm_Z_p75.tif',
        'elev_P80_2plus_30METERS.tif': 'm_Z_p80.tif',
        'elev_P90_2plus_30METERS.tif': 'm_Z_p90.tif',
        'elev_P95_2plus_30METERS.tif': 'm_Z_p95.tif',
        'elev_P99_2plus_30METERS.tif': 'm_Z_p99.tif',
        'elev_ave_2plus_30METERS.tif': 'm_Z_mean.tif',
        'elev_cubic_mean_30METERS.tif': 'm_Z_cumean.tif',
        'elev_kurtosis_2plus_30METERS.tif': 'm_Z_kurtosis.tif',
        'elev_max_2plus_30METERS.tif': 'm_Z_max.tif',
        'elev_min_2plus_30METERS.tif': 'm_Z_min.tif',
        'elev_mode_2plus_30METERS.tif': 'm_Z_mode.tif',
        'elev_quadratic_mean_30METERS.tif': 'm_Z_sqmean.tif',
        'elev_skewness_2plus_30METERS.tif': 'm_Z_skewness.tif',
        'elev_stddev_2plus_30METERS.tif': 'm_Z_stddev.tif',
        'elev_variance_2plus_30METERS.tif': 'm_Z_variance.tif',
        'int_AAD_2plus_30METERS.tif': 'm_Intensity_aad.tif',
        'int_CV_2plus_30METERS.tif': 'm_Intensity_cv.tif',
        'int_IQ_2plus_30METERS.tif': 'm_Intensity_iq.tif',
        'int_L1_2plus_30METERS.tif': 'm_Intensity_l1.tif',
        'int_L2_2plus_30METERS.tif': 'm_Intensity_l2.tif',
        'int_L3_2plus_30METERS.tif': 'm_Intensity_l3.tif',
        'int_L4_2plus_30METERS.tif': 'm_Intensity_l4.tif',
        'int_LCV_2plus_30METERS.tif': 'm_Intensity_lcv.tif',
        'int_Lkurtosis_2plus_30METERS.tif': 'm_Intensity_lkurtosis.tif',
        'int_Lskewness_2plus_30METERS.tif': 'm_Intensity_lskewness.tif',
        'int_P01_2plus_30METERS.tif': 'm_Intensity_p01.tif',
        'int_P05_2plus_30METERS.tif': 'm_Intensity_p05.tif',
        'int_P10_2plus_30METERS.tif': 'm_Intensity_p10.tif',
        'int_P20_2plus_30METERS.tif': 'm_Intensity_p20.tif',
        'int_P25_2plus_30METERS.tif': 'm_Intensity_p25.tif',
        'int_P30_2plus_30METERS.tif': 'm_Intensity_p30.tif',
        'int_P40_2plus_30METERS.tif': 'm_Intensity_p40.tif',
        'int_P50_2plus_30METERS.tif': 'm_Intensity_p50.tif',
        'int_P60_2plus_30METERS.tif': 'm_Intensity_p60.tif',
        'int_P70_2plus_30METERS.tif': 'm_Intensity_p70.tif',
        'int_P75_2plus_30METERS.tif': 'm_Intensity_p75.tif',
        'int_P80_2plus_30METERS.tif': 'm_Intensity_p80.tif',
        'int_P90_2plus_30METERS.tif': 'm_Intensity_p90.tif',
        'int_P95_2plus_30METERS.tif': 'm_Intensity_p95.tif',
        'int_P99_2plus_30METERS.tif': 'm_Intensity_p99.tif',
        'int_ave_2plus_30METERS.tif': 'm_Intensity_mean.tif',
        'int_kurtosis_2plus_30METERS.tif': 'm_Intensity_kurtosis.tif',
        'int_max_2plus_30METERS.tif': 'm_Intensity_max.tif',
        'int_min_2plus_30METERS.tif': 'm_Intensity_min.tif',
        'int_mode_2plus_30METERS.tif': 'm_Intensity_mode.tif',
        'int_skewness_2plus_30METERS.tif': 'm_Intensity_skewness.tif',
        'int_stddev_2plus_30METERS.tif': 'm_Intensity_stddev.tif',
        'int_variance_2plus_30METERS.tif': 'm_Intensity_variance.tif',
        # TODO: finish incorporating these from FUSION
        # '1st_cnt_above2_30METERS.tif': 'm_ReturnNumber_1st_count_above_htbreak.tif',  # noqa: E501
        # '1st_cnt_above_mean_30METERS.tif': 'm_ReturnNumber_1st_count_above_mean.tif', # noqa: E501
        # '1st_cnt_above_mode_30METERS.tif': 'm_ReturnNumber_1st_count_above_mode.tif', # noqa: E501
        # '1st_cover_above2_30METERS.tif': 'm_ReturnNumber_cover_above_htbreak.tif', # noqa: E501
        # '1st_cover_above_mean_30METERS.tif': 'm_ReturnNumber_cover_above_mean.tif', # noqa: E501
        # '1st_cover_above_mode_30METERS.tif': 'm_ReturnNumber_cover_above_mode.tif', # noqa: E501
        # 'all_1st_cover_above2_30METERS.tif': '',
        # 'all_1st_cover_above_mean_30METERS.tif': '',
        # 'all_1st_cover_above_mode_30METERS.tif': '',
        # 'all_cnt_2plus_30METERS.tif': 'm_ReturnNumber_all_count_above_minht.tif', # noqa: E501
        # 'all_cnt_30METERS.tif': 'm_ReturnNumber_all_count.tif',
        # 'all_cnt_above2_30METERS.tif': 'm_ReturnNumber_all_count_above_htbreak.tif', # noqa: E501
        # 'all_cnt_above_mean_30METERS.tif': 'm_ReturnNumber_all_count_above_mean.tif', # noqa: E501
        # 'all_cnt_above_mode_30METERS.tif': 'm_ReturnNumber_all_count_above_mode.tif', # noqa: E501
        # 'all_cover_above2_30METERS.tif': 'm_ReturnNumber_all_cover.tif',
        # 'all_cover_above_mean_30METERS.tif': 'm_ReturnNumber_all_cover_above_mean.tif', # noqa: E501
        # 'all_cover_above_mode_30METERS.tif': 'm_ReturnNumber_all_cover_above_mode.tif', # noqa: E501
        # 'profile_area_30METERS.tif': 'm_Z_profile_area.tif',
        # 'pulsecnt_30METERS.tif': '',
        # 'r1_cnt_2plus_30METERS.tif': 'm_ReturnNumber_r1_count.tif',
        # 'r2_cnt_2plus_30METERS.tif': 'm_ReturnNumber_r2_count.tif',
        # 'r3_cnt_2plus_30METERS.tif': 'm_ReturnNumber_r3_count.tif',
        # 'r4_cnt_2plus_30METERS.tif': 'm_ReturnNumber_r4_count.tif',
        # 'r5_cnt_2plus_30METERS.tif': 'm_ReturnNumber_r5_count.tif',
        # 'r6_cnt_2plus_30METERS.tif': 'm_ReturnNumber_r6_count.tif',
        # 'r7_cnt_2plus_30METERS.tif': 'm_ReturnNumber_r7_count.tif',
        # 'elev_canopy_relief_ratio_30METERS.tif': '',
        # 'grnd_cnt_30METERS.tif': '',
    }
    path_mapping = {}
    for k, v in mapping.items():
        new_k = os.path.join(fusion_tif_dir, k)
        new_v = os.path.join(plumas_tif_dir, v)
        path_mapping[new_k] = new_v

    yield path_mapping
