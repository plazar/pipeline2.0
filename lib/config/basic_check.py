import config_types

basic = config_types.ConfigList('basic')
basic.add_config('institution', config_types.StrConfig())
basic.add_config('pipeline', config_types.StrConfig())
basic.add_config('survey', config_types.StrConfig())
basic.add_config('pipelinedir', config_types.DirConfig())
basic.add_config('delete_rawdata', config_types.BoolConfig())
basic.add_config('coords_table', config_types.FileConfig())
