import config_types

email = config_types.ConfigList('email')
email.add_config('enabled', config_types.BoolConfig())
email.add_config('smtp_host', config_types.StrOrNoneConfig())
email.add_config('smtp_username', config_types.StrConfig())
email.add_config('smtp_password', config_types.StrConfig())
email.add_config('recipient', config_types.StrConfig())
email.add_config('sender', config_types.StrOrNoneConfig())
email.add_config('send_on_failures', config_types.BoolConfig())
email.add_config('send_on_terminal_failures', config_types.BoolConfig())
email.add_config('send_on_crash', config_types.BoolConfig())
