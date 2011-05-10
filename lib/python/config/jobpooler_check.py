#!/usr/bin/env python
import config_types

jobpooler = config_types.ConfigList('jobpooler')
jobpooler.add_config('max_jobs_running', config_types.IntConfig())
jobpooler.add_config('max_jobs_queued', config_types.PosIntConfig())
jobpooler.add_config('max_attempts', config_types.IntConfig())
jobpooler.add_config('queue_manager', config_types.QManagerConfig())

if __name__=='__main__':
    import jobpooler as configs
    jobpooler.populate_configs(configs.__dict__)
    jobpooler.check_sanity()
