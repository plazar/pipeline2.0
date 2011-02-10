import sqlite3
import config
import time
import os

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

def query(query_string):
    not_connected = True
    while not_connected:
        try:
            db_conn = sqlite3.connect(config.bgs_db_file_path,timeout=40.0)
            db_conn.row_factory = sqlite3.Row
            db_cur = db_conn.cursor()
            db_cur.execute(query_string)
            if db_cur.lastrowid:
                results = db_cur.lastrowid
            else:
                results = db_cur.fetchall()
            db_conn.commit()
            db_conn.close()
            not_connected = False
        except Exception, e:
            try:
                db_conn.close()
            except Exception:
                pass
            print "Couldn't connect to DB retrying in 1 sec.: %s" % str(e)
            time.sleep(1)
    return results
 
if not os.path.exists(config.bgs_db_file_path):
    print "Database file %s doesn't exists, creating a clean database." % \
                    config.bgs_db_file_path
    for table in creates:
        query(table)
else:
    print "Database file %s already exists. " \
            "Aborting creation of database." % config.bgs_db_file_path
