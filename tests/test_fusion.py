import os

from osgeo import gdal
import numpy as np

import silvimetric as sm


class TestFusion:
    """
    Test against Bob's FUSION data using
    NoCAL_PlumasNF_B2_2018_TestingData_FUSIONNormalized.copc.laz.
    """
    # Intensity values and Mode values are slightly off from FUSION.
    # Intensity values are scaled in FUSION, and Mode is selected slightly
    # differently, so values are expected to be off.
    def test_against_fusion(
        self,
        # configure_dask: None,
        threaded_dask,
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
            # here is where intensity values are turned off
            if 'int' in f_path:
                continue

            # we know modes are slightly different between fusion and sm, so
            # anything that depends on mode will be off
            if 'mode' in f_path:
                continue

            sm_raster = gdal.Open(sm_path)
            sm_raster_data = np.array(sm_raster.GetRasterBand(1).ReadAsArray())

            f_raster = gdal.Open(f_path)
            f_raster_data = np.array(f_raster.GetRasterBand(1).ReadAsArray())

            # Add a row to the fusion data to match raster data
            padded_fusion = np.empty(
                sm_raster_data.shape, dtype=f_raster_data.dtype
            )
            padded_fusion.fill(np.nan)
            xshape = f_raster_data.shape[0]
            yshape = f_raster_data.shape[1]
            padded_fusion[:xshape, :yshape] = f_raster_data

            # check differences and percent differences
            diff_data = np.abs(padded_fusion - sm_raster_data)
            pct_change = np.nan_to_num(diff_data / padded_fusion, 0)

            if 'cover' in f_path:
                # make sure cover differences are less than 5%
                tester = np.nan_to_num(diff_data, 0) >= 5
                if np.any(tester):
                    if diff_data[tester].size > 5:
                        failures.append(sm_path)
                        failure_cell_count.append(
                            diff_data[~(np.nan_to_num(diff_data, 0) < 5)].size
                        )
                        failure_cell_avg.append(
                            diff_data[~(np.nan_to_num(diff_data, 0) < 5)].mean()
                        )
            elif 'elev' in f_path and all(
                [v not in f_path for v in ['max', 'min', 'cv']]
            ):
                # make sure elevation differences are less than 0.2 meters
                tester = np.nan_to_num(diff_data, 0) >= 0.2
                if np.any(tester):
                    if diff_data[tester].size > 5:
                        failures.append(sm_path)
                        failure_cell_count.append(
                            diff_data[~(np.nan_to_num(diff_data, 0) < 0.2)].size
                        )
                        failure_cell_avg.append(
                            diff_data[
                                ~(np.nan_to_num(diff_data, 0) < 0.2)
                            ].mean()
                        )
            else:
                # make sure others are off by less than 5%
                tester = pct_change > 0.05
                if np.any(tester):
                    # only add if a significant number of cells are off
                    if pct_change[tester].size > 5:
                        failures.append(sm_path)
                        failure_cell_count.append(pct_change[tester].size)
                        failure_cell_avg.append(pct_change[tester].mean())

        for idx, f in enumerate(failures):
            print('Failed:')
            print('    path: ', os.path.basename(f))
            print('    count:', failure_cell_count[idx])
            print('    avg:', failure_cell_avg[idx])
        assert not failures
