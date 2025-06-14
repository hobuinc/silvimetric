from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
import numpy as np


class Test_Configuration(object):
    def test_serialization(
        self,
        storage_config: StorageConfig,
        shatter_config: ShatterConfig,
        extract_config: ExtractConfig,
    ):
        # storage
        j = str(storage_config)
        c = StorageConfig.from_string(j)

        mean = [m for m in c.metrics if m.name == 'mean']
        assert len(mean) == 1

        assert int(mean[0]._method(np.array([2, 2, 2, 2]))) == 2
        cd = c.to_json()
        scd = storage_config.to_json()
        cd.pop('log')
        scd.pop('log')
        assert scd == cd

        # shatter
        sh_str = str(shatter_config)
        sh_cfg = ShatterConfig.from_string(sh_str)
        assert shatter_config == sh_cfg

        # extract
        ex_str = str(extract_config)
        ex_cfg = ExtractConfig.from_string(ex_str)
        assert extract_config == ex_cfg
