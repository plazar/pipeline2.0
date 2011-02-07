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
import time

import header
import candidate_uploader
import diagnostic_uploader
import upload

from mailer import ErrorMailer

class JobUploader():
    
    def __init__(self):
        self.created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    def run(self):
        self.create_new_uploads()
        self.check_new_uploads()
        self.upload_checked()
        time.sleep(300)
    
    def upload_checked(self):
        checked_uploads = self.query("SELECT jobs.*,job_submits.output_dir,job_submits.base_output_dir FROM jobs,job_uploads,job_submits WHERE job_uploads.status='checked' AND jobs.id=job_uploads.job_id AND job_submits.job_id=jobs.id")
        
        for job_row in checked_uploads:
            header_id = self.header_upload(job_row,commit=True)
            if header_id:
                print "Header Uploaded id: %u" % int(header_id)
                if self.candidates_upload(job_row, header_id,commit=True):
                    print "Candidates uploaded for: %u" % int(header_id)
                    if self.diagnostics_upload(job_row,commit=True):
                       print "Diagnostics uploaded for: %u" % int(header_id)
                       last_upload_try_id = self.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
                       self.query("UPDATE job_uploads SET status='uploaded' WHERE id=%u" % last_upload_try_id)
                       self.clean_up(job_row)
    

    
    def header_upload(self,job_row,commit=False):
    	dry_run = not commit;
        
        if(dry_run):
            check_or_upload='check'
        else:
            check_or_upload='upload'
        
    	file_names_stra = self.get_jobs_files(job_row) 

        last_upload_try_id = self.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
        if file_names_stra != list():
            try:
                header_id = header.upload_header(fns=file_names_stra,dry_run=dry_run)
            except header.HeaderError, e:
                print "Header Uploader Parsing error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
                self.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
                % ('Header %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')) ,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
                try:
                    notification = ErrorMailer('Header %s failed: %s' % (check_or_upload,str(e)))
                    notification.send()
                except Exception,e:
                    pass
                return False
            except upload.UploadError, e:
                print "Header Uploader error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
                self.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u" % ('Header uploader error (probable connection issues)',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
                try:
                    notification = ErrorMailer('Header %s failed: %s' % (check_or_upload,str(e)))
                    notification.send()
                except Exception,e:
                    pass
                return False
            
            self.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u"\
                % ('Header %s' % check_or_upload ,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
                
            if(dry_run):
                print "Header check success for jobs.id: %u \tjob_uploads.id:%u" % (int(job_row['id']), int(last_upload_try_id))
            else:
                return header_id
                print "Header upload success for jobs.id: %u \tjob_uploads.id:%u \theader_id: %u" % (int(job_row['id']), int(last_upload_try_id),int(header_id))
                
            
            return True
        else:
            print "No files were found in database for jobs.id: %u \tjob_uploads.id:%u" % (int(job_row['id']), int(last_upload_try_id))
            self.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
                % ('No files were found in database for this job',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
            return False 
    
    def candidates_upload(self,job_row,header_id=0,commit=False):
        dry_run = not commit;
        
        if(dry_run):
            check_or_upload='check'
        else:
            check_or_upload='upload'
        
        if(config.uploader_result_dir_overide):
            moded_dir = job_row['output_dir'].replace(job_row['base_output_dir'],config.uploader_result_dir)
        else:
            moded_dir = job_row['output_dir']
            
        last_upload_try_id = self.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
        
        
        try:
            candidate_uploader.upload_candidates(header_id=header_id, versionnum=config.uploader_version_num,  directory=moded_dir,dry_run=dry_run)
        except candidate_uploader.PeriodicityCandidateError, e:
            print "Candidates Uploader Parsing error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
            self.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
            try:
                notification = ErrorMailer('Candidates %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False
        except upload.UploadError, e:
            print "Candidates Uploader error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
            self.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u" % ('Candidates uploader error (probable connection issues)',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
            try:
                notification = ErrorMailer('Candidates %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False

        print "Candidates %s success for jobs.id: %u \tjob_uploads.id:%u" % (check_or_upload,int(job_row['id']), int(last_upload_try_id))
        self.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates %s' % check_or_upload,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
        return True

            
    def diagnostics_upload(self,job_row,commit=False):
        dry_run = not commit;
        
        if(dry_run):
            check_or_upload='check'
        else:
            check_or_upload='upload'
        
        if(config.uploader_result_dir_overide):
            moded_dir = job_row['output_dir'].replace(job_row['base_output_dir'],config.uploader_result_dir)
        else:
            moded_dir = job_row['output_dir']
        
        last_upload_try_id = self.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
        obs_name = moded_dir.split('/')[len(moded_dir.split('/'))-3]
        beamnum = int(moded_dir.split('/')[len(moded_dir.split('/'))-2])
        print "obs_name: %s  beamnum: %s" % (obs_name,beamnum)
        
        try:
            diagnostic_uploader.upload_diagnostics(obsname=obs_name,beamnum=beamnum, versionnum=config.uploader_version_num,  directory=moded_dir,dry_run=dry_run)
        except header.DiagnosticError, e:
            print "Diagnostics Uploader Parsing error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
            self.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Diagnostics %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
            try:
                notification = ErrorMailer('Diagnostics %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False
        except upload.UploadError, e:
            print "Diagnostics Uploader error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
            self.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u" % ('Diagnostics uploader error (probable connection issues)',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
            try:
                notification = ErrorMailer('Diagnostics %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False

        print "Diagnostics %s success for jobs.id: %u \tjob_uploads.id:%u" % (check_or_upload,int(job_row['id']), int(last_upload_try_id))
        self.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u"\
            % ('Diagnostics %s' % check_or_upload ,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_upload_try_id))
        return True
        
              
    def create_new_uploads(self):        
        print "Creating new upload entries..."
        jobs_with_no_uploads = self.query("SELECT * FROM jobs WHERE status='processed' AND id NOT IN (SELECT job_id FROM job_uploads)")
        for job_row in jobs_with_no_uploads:
            self.query("INSERT INTO job_uploads (job_id, status, details, created_at, updated_at) VALUES(%u,'%s','%s','%s','%s')"\
                % (job_row['id'], 'new','Newly added upload',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            
    def check_new_uploads(self):
        new_uploads = self.query("SELECT jobs.*,job_submits.output_dir,job_submits.base_output_dir FROM jobs,job_uploads,job_submits WHERE job_uploads.status='new' AND jobs.id=job_uploads.job_id AND job_submits.job_id=jobs.id")
        for job_row in new_uploads:
            if self.header_upload(job_row):
                print "Header check passed"
                if self.candidates_upload(job_row):
                    print "Candidates check passed"
                    if self.diagnostics_upload(job_row):
                       print "Diagnostics check passed"
                       last_upload_try_id = self.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
                       self.query("UPDATE job_uploads SET status='checked' WHERE id=%u" % last_upload_try_id)

    def clean_up(self,job_row):
        downloads = self.query('SELECT downloads.* FROM jobs,job_files,downloads WHERE jobs.id=%u AND jobs.id=job_files.job_id AND job_files.file_id=downloads.id' % (job_row['id']))
        for download in downloads:
            if os.path.exists(download['filename']):
                os.remove(download['filename'])
                print "Deleted: %s" % download['filename']
        
    def upload_job(self,job_row):
        print "Header checked: %s" % str()
        print "Candidates checked: %s" % str()
            #except Exception, e:
            #    print "Error Error!"
                
        #header.upload_header(fns=file_names_stra)

                           
    def get_processed_jobs(self):
        return self.query("SELECT * FROM jobs WHERE status='processed'")
        
    def get_upload_attempts(self,job_row):
        return self.query("SELECT * FROM job_uploads WHERE job_id = %u" % int(job_row['id']))
            
    def get_jobs_files(self,job_row):
        file_rows = self.query("SELECT * FROM job_files,downloads WHERE job_files.job_id=%u AND downloads.id=job_files.file_id" % int(job_row['id']))
        files_stra = list()
        for file_row in file_rows:
            if(config.uploader_result_dir_overide):
                if(job_row['base_output_dir']):
                    overriden = self.overide_base_results_directory(job_row['base_output_dir'],file_row['filename'],job_row['output_dir'])
                    if (os.path.exists(overriden)):
                        files_stra.append(overriden)
            else:
                if (os.path.exists(file_row['filename'])):
                    files_stra.append(file_row['filename'])
        return files_stra
    
    def overide_base_results_directory(self,base_dir, filename,output_dir):
        return os.path.join(output_dir.replace(base_dir,config.uploader_result_dir),os.path.basename(filename))
    
    def clean(self):
        #remove downloaded files
        uploaded_jobs = self.query("SELECT jobs.*,job_submits.output_dir FROM jobs,job_uploads,job_submits WHERE job_uploads.status='uploaded' AND jobs.id=job_uploads.job_id AND job_submits.job_id=jobs.id")
        
        for job_row in uploaded_jobs:
            for file_path in self.get_jobs_files(job_row):
                if os.path.exists(file_path):
                    os.remove(file_path)
    

    def query(self,query_string):
        not_connected = True
        while not_connected:
            try:
                db_conn = sqlite3.connect(config.bgs_db_file_path,timeout=40.0);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
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
                except Exception, e:
                    pass
                print "Couldn't connect to DB retrying in 1 sec.: %s" % str(e) 
                time.sleep(1)
        return results
