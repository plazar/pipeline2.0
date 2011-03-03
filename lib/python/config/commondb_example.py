################################################################
# Common Database Configuration
################################################################
username = 'username'
password = 'password'
host = 'arecibosql.tc.cornell.edu'

import commondb_check
commondb_check.commondb.populate_configs(locals())
commondb_check.commondb.check_sanity()
