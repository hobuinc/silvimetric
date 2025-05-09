import os

import silvimetric as sm
from osgeo import gdal
import numpy as np


# Test against Bob's FUSION data
class TestFusion:
    def test_cover(
        self,
        configure_dask: None,
        plumas_shatter_config: sm.ShatterConfig,
        plumas_tif_dir: str,
        metric_map: dict,
    ):
        pl_tdb_dir = plumas_shatter_config.tdb_dir
        sm.shatter(plumas_shatter_config)
        ec = sm.ExtractConfig(tdb_dir=pl_tdb_dir, out_dir=plumas_tif_dir)
        sm.extract(ec)
        failures = []
        failure_cell_count = []
        failure_cell_avg = []
        for f_path, sm_path in metric_map.items():
            sm_raster = gdal.Open(sm_path)
            sm_raster_data = np.array(sm_raster.GetRasterBand(1).ReadAsArray())

            f_raster = gdal.Open(f_path)
            f_raster_data = np.array(f_raster.GetRasterBand(1).ReadAsArray())

            # TODO add a row to the fusion data to match raster data
            padded_fusion = np.empty(
                sm_raster_data.shape, dtype=f_raster_data.dtype
            )
            padded_fusion.fill(np.nan)
            xshape = f_raster_data.shape[0]
            yshape = f_raster_data.shape[1]
            padded_fusion[:xshape, :yshape] = f_raster_data
            diff_data = np.abs(padded_fusion - sm_raster_data)

            if 'cover' in f_path:
                # make sure cover differences are less than 5%
                if not np.all(np.nan_to_num(diff_data, 0) < 5):
                    failures.append(sm_path)
                    failure_cell_count.append(
                        diff_data[~(np.nan_to_num(diff_data, 0) < 5)].size
                    )
                    failure_cell_avg.append(
                        diff_data[~(np.nan_to_num(diff_data, 0) < 5)].mean()
                    )
            elif (
                'elev' in f_path and 'max' not in f_path and 'min' not in f_path
            ):
                # make sure elevation differences are less than 0.2 meters
                if not np.all(np.nan_to_num(diff_data, 0) < 0.2):
                    failures.append(sm_path)
                    failure_cell_count.append(
                        diff_data[~(np.nan_to_num(diff_data, 0) < 0.2)].size
                    )
                    failure_cell_avg.append(
                        diff_data[~(np.nan_to_num(diff_data, 0) < 0.2)].mean()
                    )
            else:
                # make sure others have difference less than 1
                if not np.all(np.nan_to_num(diff_data, 0) < 1):
                    failures.append(sm_path)
                    failure_cell_count.append(
                        diff_data[~(np.nan_to_num(diff_data, 0) < 1)].size
                    )
                    failure_cell_avg.append(
                        diff_data[~(np.nan_to_num(diff_data, 0) < 1)].mean()
                    )

        for idx, f in enumerate(failures):
            print('Failed:')
            print('    path: ', os.path.basename(f))
            print('    count:', failure_cell_count[idx])
            print('    avg:', failure_cell_avg[idx])
        assert not failures
