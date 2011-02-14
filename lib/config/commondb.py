################################################################
# Common Database Configuration
################################################################
username = 'mcgill'
password = 'pw4sd2mcgill!'
host = 'arecibosql.tc.cornell.edu'

import commondb_check
commondb_check.commondb.populate_configs(locals())
commondb_check.commondb.check_sanity()
