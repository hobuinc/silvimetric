import os

import silvimetric as sm
import rasterio
import numpy as np

# Test against Bob's FUSION data
class TestFusion():
    def test_cover(self, process_dask, fusion_data, plumas_shatter_config, plumas_tif_dir, plumas_cover_file):
        pl_tdb_dir = plumas_shatter_config.tdb_dir
        sm.shatter(plumas_shatter_config)
        ec = sm.ExtractConfig(tdb_dir=pl_tdb_dir, out_dir=plumas_tif_dir)
        sm.extract(ec)
        raster = rasterio.open(plumas_cover_file)
        raster_data = raster.read(1)

        #TODO add a row to the fusion data to match raster data
        padded_fusion = np.empty(raster_data.shape, dtype=fusion_data.dtype)
        padded_fusion.fill(np.nan)
        xshape = fusion_data.shape[0]
        yshape = fusion_data.shape[1]
        padded_fusion[:xshape, :yshape] = fusion_data
        diff_data= np.abs(padded_fusion - raster_data)

        assert np.all(np.nan_to_num(diff_data,0) < 1)
