#!/usr/bin/env python
import config_types

processing = config_types.ConfigList('processing')
processing.add_config('base_results_directory', config_types.ReadWriteConfig())
processing.add_config('base_working_directory', config_types.StrConfig())
processing.add_config('base_tmp_dir', config_types.StrConfig())
processing.add_config('default_zaplist', config_types.FileConfig())
processing.add_config('zaplistdir', config_types.DirConfig())
processing.add_config('num_cores', config_types.PosIntConfig())
processing.add_config('use_hyperthreading', config_types.BoolConfig())

if __name__=='__main__':
    import processing as configs
    processing.populate_configs(configs.__dict__)
    processing.check_sanity()
