#!/usr/bin/env python
import config_types

email = config_types.ConfigList('email')
email.add_config('enabled', config_types.BoolConfig())
email.add_config('smtp_host', config_types.StrOrNoneConfig())
email.add_config('smtp_port', config_types.IntConfig())
email.add_config('smtp_username', config_types.StrConfig())
email.add_config('smtp_password', config_types.StrConfig())
email.add_config('smtp_login', config_types.BoolConfig())
email.add_config('smtp_usetls', config_types.BoolConfig())
email.add_config('smtp_usessl', config_types.BoolConfig())
email.add_config('recipient', config_types.StrConfig())
email.add_config('send_on_failures', config_types.BoolConfig())
email.add_config('send_on_terminal_failures', config_types.BoolConfig())
email.add_config('send_on_crash', config_types.BoolConfig())

if __name__=='__main__':
    import email as configs
    email.populate_configs(configs.__dict__)
    email.check_sanity()
