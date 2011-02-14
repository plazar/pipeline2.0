import config_types

background = config_types.ConfigList('background')
background.add_config('screen_output', config_types.BoolConfig())
background.add_config('jobtracker_db', config_types.FileConfig())
