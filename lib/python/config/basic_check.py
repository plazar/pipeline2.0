#!/usr/bin/env python
import config_types

basic = config_types.ConfigList('basic')
basic.add_config('institution', config_types.StrConfig())
basic.add_config('pipeline', config_types.StrConfig())
basic.add_config('survey', config_types.StrConfig())
basic.add_config('pipelinedir', config_types.DirConfig())
basic.add_config('delete_rawdata', config_types.BoolConfig())
basic.add_config('coords_table', config_types.FileConfig())
basic.add_config('log_dir', config_types.ReadWriteDirConfig())
basic.add_config('qsublog_dir', config_types.ReadWriteDirConfig())

if __name__=='__main__':
    import basic as configs
    basic.populate_configs(configs.__dict__)
    basic.check_sanity()
