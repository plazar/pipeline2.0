import os
import time
import warnings

import candidate_uploader
import diagnostic_uploader
import upload
import mailer
import jobtracker
import header
import config.upload
import config.jobpooler

# Suppress warnings produced by uploaders
# (typically because data, weights, scales, offsets are missing 
#       from PSRFITS files)
warnings.filterwarnings("ignore", message="Can't find the 'DATA' column!")
warnings.filterwarnings("ignore", message="Can't find the channel weights column, 'DAT_WTS'!")
warnings.filterwarnings("ignore", message="Can't find the channel offsets column, 'DAT_OFFS'!")
warnings.filterwarnings("ignore", message="Can't find the channel scalings column, 'DAT_SCL'!")

class JobUploader():
    
    def __init__(self):
        self.created_at = jobtracker.nowstr()

    def run(self):
        self.create_new_uploads()
        self.check_new_uploads()
        self.upload_checked()
        time.sleep(300)
    
    def upload_checked(self):
        checked_uploads = jobtracker.query("SELECT jobs.*,job_submits.output_dir,job_submits.base_output_dir FROM jobs,job_uploads,job_submits WHERE job_uploads.status='checked' AND jobs.id=job_uploads.job_id AND job_submits.job_id=jobs.id")
        
        for job_row in checked_uploads:
            header_id = self.header_upload(job_row,commit=True)
            if header_id:
                print "Header Uploaded id: %u" % int(header_id)
                if self.candidates_upload(job_row, header_id,commit=True):
                    print "Candidates uploaded for: %u" % int(header_id)
                    if self.diagnostics_upload(job_row,commit=True):
                       print "Diagnostics uploaded for: %u" % int(header_id)
                       last_upload_try_id = jobtracker.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
                       jobtracker.query("UPDATE job_uploads SET status='uploaded' WHERE id=%u" % last_upload_try_id)
                       self.clean_up(job_row)
    
    def header_upload(self,job_row,commit=False):
    	dry_run = not commit;
        
        if(dry_run):
            check_or_upload='check'
        else:
            check_or_upload='upload'
        
    	file_names_stra = self.get_jobs_files(job_row) 

        last_upload_try_id = jobtracker.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
        if file_names_stra != list():
            try:
                header_id = header.upload_header(fns=file_names_stra,dry_run=dry_run)
            except header.HeaderError, e:
                print "Header Uploader Parsing error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
                jobtracker.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
                % ('Header %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')) ,jobtracker.nowstr(), last_upload_try_id))
                try:
                    notification = mailer.ErrorMailer('Header %s failed: %s' % (check_or_upload,str(e)))
                    notification.send()
                except Exception,e:
                    pass
                return False
            except upload.UploadError, e:
                print "Header Uploader error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
                jobtracker.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u" % ('Header uploader error (probable connection issues)',jobtracker.nowstr(), last_upload_try_id))
                try:
                    notification = mailer.ErrorMailer('Header %s failed: %s' % (check_or_upload,str(e)))
                    notification.send()
                except Exception,e:
                    pass
                return False
            
            jobtracker.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u"\
                % ('Header %s' % check_or_upload ,jobtracker.nowstr(), last_upload_try_id))
                
            if(dry_run):
                print "Header check success for jobs.id: %u \tjob_uploads.id:%u" % (int(job_row['id']), int(last_upload_try_id))
            else:
                return header_id
                print "Header upload success for jobs.id: %u \tjob_uploads.id:%u \theader_id: %u" % (int(job_row['id']), int(last_upload_try_id),int(header_id))
            return True
        else:
            print "No files were found in database for jobs.id: %u \tjob_uploads.id:%u" % (int(job_row['id']), int(last_upload_try_id))
            jobtracker.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
                % ('No files were found in database for this job',jobtracker.nowstr(), last_upload_try_id))
            return False 
    
    def candidates_upload(self,job_row,header_id=0,commit=False):
        dry_run = not commit;
        
        if(dry_run):
            check_or_upload='check'
        else:
            check_or_upload='upload'
        
        dir = job_row['output_dir']
            
        last_upload_try_id = jobtracker.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
                
        try:
            candidate_uploader.upload_candidates(header_id=header_id, versionnum=config.upload.version_num,  directory=dir,dry_run=dry_run)
        except candidate_uploader.PeriodicityCandidateError, e:
            print "Candidates Uploader Parsing error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
            jobtracker.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),jobtracker.nowstr(), last_upload_try_id))
            try:
                notification = mailer.ErrorMailer('Candidates %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False
        except upload.UploadError, e:
            print "Candidates Uploader error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
            jobtracker.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u" % ('Candidates uploader error (probable connection issues)',jobtracker.nowstr(), last_upload_try_id))
            try:
                notification = mailer.ErrorMailer('Candidates %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False

        print "Candidates %s success for jobs.id: %u \tjob_uploads.id:%u" % (check_or_upload,int(job_row['id']), int(last_upload_try_id))
        jobtracker.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates %s' % check_or_upload,jobtracker.nowstr(), last_upload_try_id))
        return True
            
    def diagnostics_upload(self,job_row,commit=False):
        dry_run = not commit;
        
        if(dry_run):
            check_or_upload='check'
        else:
            check_or_upload='upload'
        
        dir = job_row['output_dir']
        
        last_upload_try_id = jobtracker.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
        obs_name = dir.split('/')[len(dir.split('/'))-3]
        beamnum = int(dir.split('/')[len(dir.split('/'))-2])
        print "obs_name: %s  beamnum: %s" % (obs_name,beamnum)
        
        try:
            diagnostic_uploader.upload_diagnostics(obsname=obs_name,beamnum=beamnum, versionnum=config.upload.version_num,  directory=dir,dry_run=dry_run)
        except header.DiagnosticError, e:
            print "Diagnostics Uploader Parsing error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
            jobtracker.query("UPDATE job_uploads SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Diagnostics %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),jobtracker.nowstr(), last_upload_try_id))
            try:
                notification = mailer.ErrorMailer('Diagnostics %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False
        except upload.UploadError, e:
            print "Diagnostics Uploader error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_row['id']), int(last_upload_try_id))
            jobtracker.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u" % ('Diagnostics uploader error (probable connection issues)',jobtracker.nowstr(), last_upload_try_id))
            try:
                notification = mailer.ErrorMailer('Diagnostics %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False

        print "Diagnostics %s success for jobs.id: %u \tjob_uploads.id:%u" % (check_or_upload,int(job_row['id']), int(last_upload_try_id))
        jobtracker.query("UPDATE job_uploads SET details='%s', updated_at='%s' WHERE id=%u"\
            % ('Diagnostics %s' % check_or_upload ,jobtracker.nowstr(), last_upload_try_id))
        return True       
              
    def create_new_uploads(self):        
        print "Creating new upload entries..."
        jobs_with_no_uploads = jobtracker.query("SELECT * FROM jobs WHERE status='processed' AND id NOT IN (SELECT job_id FROM job_uploads)")
        for job_row in jobs_with_no_uploads:
            jobtracker.query("INSERT INTO job_uploads (job_id, status, details, created_at, updated_at) VALUES(%u,'%s','%s','%s','%s')"\
                % (job_row['id'], 'new','Newly added upload',jobtracker.nowstr(),jobtracker.nowstr()))
            
    def check_new_uploads(self):
        new_uploads = jobtracker.query("SELECT jobs.*,job_submits.output_dir,job_submits.base_output_dir FROM jobs,job_uploads,job_submits WHERE job_uploads.status='new' AND jobs.id=job_uploads.job_id AND job_submits.job_id=jobs.id")
        for job_row in new_uploads:
            if self.header_upload(job_row):
                print "Header check passed"
                if self.candidates_upload(job_row):
                    print "Candidates check passed"
                    if self.diagnostics_upload(job_row):
                       print "Diagnostics check passed"
                       last_upload_try_id = jobtracker.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
                       jobtracker.query("UPDATE job_uploads SET status='checked' WHERE id=%u" % last_upload_try_id)

    def clean_up(self,job_row):
        downloads = jobtracker.query('SELECT downloads.* FROM jobs,job_files,downloads WHERE jobs.id=%u AND jobs.id=job_files.job_id AND job_files.file_id=downloads.id' % (job_row['id']))
        for download in downloads:
            if config.jobpooler.delete_rawdata and os.path.exists(download['filename']):
                os.remove(download['filename'])
                print "Deleted: %s" % download['filename']
                           
    def get_processed_jobs(self):
        return jobtracker.query("SELECT * FROM jobs WHERE status='processed'")
        
    def get_upload_attempts(self,job_row):
        return jobtracker.query("SELECT * FROM job_uploads WHERE job_id = %u" % int(job_row['id']))
            
    def get_jobs_files(self,job_row):
        file_rows = jobtracker.query("SELECT * FROM job_files,downloads WHERE job_files.job_id=%u AND downloads.id=job_files.file_id" % int(job_row['id']))
        files_stra = list()
        for file_row in file_rows:
            if (os.path.exists(file_row['filename'])):
                files_stra.append(file_row['filename'])
        return files_stra
    
    
    def clean(self):
        uploaded_jobs = jobtracker.query("SELECT jobs.*,job_submits.output_dir FROM jobs,job_uploads,job_submits WHERE job_uploads.status='uploaded' AND jobs.id=job_uploads.job_id AND job_submits.job_id=jobs.id")
        for job_row in uploaded_jobs:
            for file_path in self.get_jobs_files(job_row):
                if config.jobpooler.delete_rawdata and os.path.exists(file_path):
                    os.remove(file_path)
