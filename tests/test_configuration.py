from silvimetric import StorageConfig, ShatterConfig, ExtractConfig

class Test_Configuration(object):

    def test_serialization(self, storage_config, shatter_config, extract_config):

        # storage
        j = str(storage_config)
        c = StorageConfig.from_string(j)
        mean = [ m for m in c.metrics if m.name == 'mean']
        assert len(mean) == 1

        assert int(mean[0]([2,2,2,2])) == 2
        assert storage_config == c

        # shatter
        sh_str = str(shatter_config)
        sh_cfg = ShatterConfig.from_string(sh_str)
        assert shatter_config == sh_cfg

        # extract
        ex_str = str(extract_config)
        ex_cfg = ExtractConfig.from_string(ex_str)
        assert extract_config == ex_cfg