from silvimetric import StorageConfig, ShatterConfig, ExtractConfig
import numpy as np
import datetime

from silvimetric.resources.storage import Storage


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
        # adding date to the config since extract creates dates if not provided
        extract_config.date = (
            datetime.datetime(2011, 1, 1),
            datetime.datetime(2012, 1, 1),
        )
        ex_str = str(extract_config)
        ex_cfg = ExtractConfig.from_string(ex_str)
        assert extract_config == ex_cfg

    def test_reusing_storage(
        self,
        storage: Storage,
        shatter_config: ShatterConfig,
        extract_config: ExtractConfig,
    ):
        """
        Make sure that we can pass Storage object to tdb_dir so that ExtractConfig
        doesn't need to make a second one when initializing.
        """
        sh_json = shatter_config.to_json()
        sh_json['tdb_dir'] = storage
        sh_config = ShatterConfig.from_dict(sh_json)

        assert sh_config == shatter_config

        ex_json = extract_config.to_json()
        ex_json['tdb_dir'] = storage
        ex_config = ExtractConfig.from_dict(ex_json)

        assert ex_config == extract_config
