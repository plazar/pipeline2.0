################################################################
# Background Script Configuration
################################################################
screen_output = True # Set to True if you want the script to
                                # output runtime information, False otherwise
# Path to sqlite3 database file
#jobtracker_db = "/data/alfa/test_pipeline_clean/storage_db"
jobtracker_db = "/homes/borgii/snipka/dev/storage_db"
sleep = 60

import background_check
background_check.background.populate_configs(locals())
background_check.background.check_sanity()
