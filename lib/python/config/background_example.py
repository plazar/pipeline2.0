################################################################
# Background Script Configuration
################################################################
screen_output = True # Set to True if you want the script to 
                     # output runtime information, False otherwise

# Path to sqlite3 database file
# This database is created by "create_database.py"
# It is needed by: the job pooler, the downloader, the uploader,
# and several utils in bin/ dir.
jobtracker_db = "/data/alfa/test_pipeline_clean/storage_db"

# This is the number of seconds to sleep between iterations of
# the background scripts loops.
sleep = 60

import background_check
background_check.background.populate_configs(locals())
background_check.background.check_sanity()
