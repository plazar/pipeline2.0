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
import os

import header
import candidate_uploader
import diagnostic_uploader

class JobUploader():
    
    def __init__(self):
        self.created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    def run(self):
        print "Creating new upload entries"
        self.create_new_uploads()
        self.check_new_uploads()
        #self.upload_checked()
        #self.retry_failed_uploads()
        #while True:
        #    sleep(15)
    
    def create_new_uploads(self):
        jobs_with_no_uploads = self.query("SELECT * FROM jobs WHERE status='processed' AND id NOT IN (SELECT job_id FROM job_uploads)")
        for job_row in jobs_with_no_uploads:
            self.query("INSERT INTO job_uploads (job_id, status, details, created_at, updated_at) VALUES(%u,'%s','%s','%s','%s')"\
                % (job_row['id'], 'new','Newly added upload',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            
    def check_new_uploads(self):
        new_uploads = self.query("SELECT jobs.*,job_submits.output_dir FROM jobs,job_uploads,job_submits WHERE job_uploads.status='new' AND jobs.id=job_uploads.job_id AND job_submits.job_id=jobs.id")
        for job_row in new_uploads:
            if self.header_dry_run(job_row):
                print "Header check passed"
                if self.candidates_dry_run(job_row):
                    print "Candidates check passed"
                    if self.diagnostics_dry_run(job_row):
                       print "Diagnostics check passed"
    def upload_job(self,job_row):
        print "Header checked: %s" % str()
        print "Candidates checked: %s" % str()
            #except Exception, e:
            #    print "Error Error!"
                
        #header.upload_header(fns=file_names_stra)
        
    def header_dry_run(self,job_row):
        file_names_stra = self.get_jobs_files(job_row)
        last_upload_try_id = self.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
        if file_names_stra != list():
            #try:
            try:
                header.upload_header(fns=file_names_stra,dry_run=True)
            except header.HeaderError, e:
                self.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
                % ('Header check failed: %s' % str(e).replace("'","").replace('"',''),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
                return False
            
            self.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u"\
                % ('Header check passed',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
            return True
        else:
            self.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
                % ('No files were found in database for this job',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
            return False 
                    
    def candidates_dry_run(self,job_row):
        file_names_stra = self.get_jobs_files(job_row)
        last_upload_try_id = self.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
        moded_dir = job_row['output_dir'].split('/')
        moded_dir[1] = 'data'
        print "/".join(moded_dir)
        
        try:
            candidate_uploader.upload_candidates(1, 'blewblahblah',  "/".join(moded_dir),dry_run=True)
        except candidate_uploader.PeriodicityCandidateError, e:
            self.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
                % ('Candidates check failed: %s' % str(e).replace("'","").replace('"',''),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
            return False
        
        self.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates check passed',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
        return True
     
    def diagnostics_dry_run(self,job_row):
        file_names_stra = self.get_jobs_files(job_row)
        last_upload_try_id = self.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
        moded_dir = job_row['output_dir'].split('/')
        moded_dir[1] = 'data'
        tmp_obs = job_row['output_dir'].split('/')
        obs_name = tmp_obs[len(moded_dir)-3]
        beamnum = int(tmp_obs[len(moded_dir)-2])
        print "obs_name: %s  beamnum: %s" % (obs_name,beamnum)
        try:
            diagnostic_uploader.upload_diagnostics(obs_name,beamnum, 'blewblahblah',  "/".join(moded_dir),dry_run=True)
        except diagnostic_uploader.DiagnosticError, e:
            print str(e)
            self.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
                % ('Diagnostics check failed: %s' % str(e).replace("'","").replace('"',''),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
            return False
        
        self.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u"\
            % ('Diagnostics check passed',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
        return True
                           
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
        not_connected = True
        while not_connected:
            try:
                #db_conn = sqlite3.connect(config.bgs_db_file_path)
                db_conn = sqlite3.connect("upload_db")
                not_connected = False
            except Exception, e:
                jobpool_cout.outs("Couldn't connect to DB retrying in 1 sec.") 
                time.sleep(1)
                          
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
            
        
    
