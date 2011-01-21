from threading import Thread
import subprocess
import sys
import exceptions
import re
import datetime
import sqlite3
import config
import header
from time import sleep

import header

class JobUploader():
    
    def __init__(self):
        self.created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    def run(self):
        while True:
            sleep(15)
    
    
    def upload_jobs_header(self,job_row):
        file_names_stra = self.get_jobs_files(job_row)
        if file_names_stra != list():
            header.upload_header(fns=file_names_stra,verbose=True,dry_run=True)
            
    def get_processed_jobs(self):
        return self.query("SELECT * FROM jobs WHERE status='processed'")
        
    def get_upload_attempts(self,job_row):
        return self.query("SELECT * FROM job_uploads WHERE job_id = %u" % int(job_row['id']))
            
    def get_jobs_files(self,job_row):
        file_rows = self.query("SELECT * FROM job_files,downloads WHERE job_files.job_id=%u AND downloads.id=job_files.file_id" % int(job_row['id']))
        files_stra = list()
        for file_row in file_rows:
            files_stra.append(file_row['filename'])
        return files_stra

    def query(self,query_string):
        db_conn = sqlite3.connect(config.bgs_db_file_path);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        db_cur.execute(query_string)
        if db_cur.lastrowid:
            results = db_cur.lastrowid
        else:
            results = db_cur.fetchall()
        db_conn.commit()
        db_conn.close()
        return results
            
        
    