import os.path

import jobtracker
import config.background
"""
This script creates sqlite3 clean database structure to be used by Pipeline2.0
"""
creates = []


creates.append("CREATE TABLE pipeline ( " \
                    "exit_downloader INTEGER NOT NULL DEFAULT (0), " \
                    "exit_jobpool INTEGER NOT NULL DEFAULT (0), " \
                    "exit_uploader INTEGER NOT NULL DEFAULT (0))")
creates.append("CREATE TABLE download_attempts ( " \
                    "download_id INTEGER, " \
                    "created_at TEXT, " \
                    "details TEXT, " \
                    "id INTEGER PRIMARY KEY, " \
                    "status TEXT, " \
                    "updated_at TEXT)")
creates.append("CREATE TABLE files ( " \
                    "created_at TEXT, " \
                    "details TEXT, " \
                    "filename TEXT, " \
                    "id INTEGER PRIMARY KEY, " \
                    "remote_filename TEXT, " \
                    "request_id INTEGER, " \
                    "status TEXT, " \
                    "updated_at TEXT, " \
                    "size INTEGER)")
creates.append("CREATE TABLE job_files ( " \
                    "file_id INTEGER, " \
                    "created_at TEXT, " \
                    "id INTEGER PRIMARY KEY, " \
                    "job_id INTEGER, " \
                    "updated_at TEXT)")
creates.append("CREATE TABLE job_submits ( " \
                    "created_at TEXT, " \
                    "details TEXT, " \
                    "id INTEGER PRIMARY KEY, " \
                    "job_id INTEGER, " \
                    "queue_id TEXT, " \
                    "status TEXT, " \
                    "updated_at TEXT, " \
                    "output_dir TEXT)")
creates.append("CREATE TABLE jobs ( " \
                    "created_at TEXT, " \
                    "details TEXT, " \
                    "id INTEGER PRIMARY KEY, " \
                    "status TEXT, " \
                    "updated_at TEXT)")
creates.append("CREATE TABLE requests ( " \
                    "size INTEGER, " \
                    "created_at TEXT, " \
                    "details TEXT, " \
                    "guid TEXT, " \
                    "id INTEGER PRIMARY KEY, " \
                    "status TEXT, " \
                    "updated_at TEXT)")

if not os.path.exists(config.background.jobtracker_db):
    print "Database file %s doesn't exist, creating a clean database." % \
                    config.background.jobtracker_db
    for table in creates:
        jobtracker.query(table)
else:
    print "Database file %s already exists. " \
            "Aborting creation of database." % config.background.jobtracker_db
