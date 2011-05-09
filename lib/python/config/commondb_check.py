#!/usr/bin/env python
import config_types

commondb = config_types.ConfigList('commondb')
commondb.add_config('username', config_types.StrConfig())
commondb.add_config('password', config_types.StrConfig())
commondb.add_config('host', config_types.StrConfig())

if __name__=='__main__':
    import commondb as configs
    commondb.populate_configs(configs.__dict__)
    commondb.check_sanity()
