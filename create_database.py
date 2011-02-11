import os.path

import jobtracker
import config

creates = []
creates.append("CREATE TABLE download_attempts ( " \
                    "download_id INTEGER, " \
                    "created_at TEXT, " \
                    "details TEXT, " \
                    "id INTEGER PRIMARY KEY, " \
                    "status TEXT, " \
                    "updated_at TEXT)")
creates.append("CREATE TABLE downloads ( " \
                    "created_at TEXT, " \
                    "details TEXT, " \
                    "filename TEXT, "\
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
creates.append("CREATE TABLE job_submits ( " 
                    "created_at TEXT, " \
                    "details TEXT, " \
                    "id INTEGER PRIMARY KEY, " \
                    "job_id INTEGER, " \
                    "queue_id TEXT, " \
                    "status TEXT, " \
                    "updated_at TEXT, " \
                    "output_dir TEXT, " \
                    "base_output_dir TEXT)")
creates.append("CREATE TABLE job_uploads ( " \
                    "created_at TEXT, " \
                    "details TEXT, " \
                    "id INTEGER PRIMARY KEY, " \
                    "job_id INTEGER, " \
                    "status TEXT, " \
                    "updated_at TEXT)")
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


if not os.path.exists(config.bgs_db_file_path):
    print "Database file %s doesn't exists, creating a clean database." % \
                    config.bgs_db_file_path
    for table in creates:
        jobtracker.query(table)
else:
    print "Database file %s already exists. " \
            "Aborting creation of database." % config.bgs_db_file_path
