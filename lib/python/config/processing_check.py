import config_types

processing = config_types.ConfigList('processing')
processing.add_config('base_working_directory', config_types.ReadWriteConfig())
processing.add_config('default_zaplist', config_types.FileConfig())
processing.add_config('zaplistdir', config_types.DirConfig())
