import config_types

commondb = config_types.ConfigList('commondb')
commondb.add_config('username', config_types.StrConfig())
commondb.add_config('password', config_types.StrConfig())
commondb.add_config('host', config_types.StrConfig())
