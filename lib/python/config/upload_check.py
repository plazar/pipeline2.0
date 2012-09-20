#!/usr/bin/python
import config_types

upload = config_types.ConfigList('upload')
upload.add_config('version_num', config_types.FuncConfig())
upload.add_config('pfd_ftp_dir', config_types.StrConfig())
upload.add_config('sp_ftp_dir', config_types.StrConfig())

if __name__=='__main__':
    import upload as configs
    upload.populate_configs(configs.__dict__)
    upload.check_sanity()
