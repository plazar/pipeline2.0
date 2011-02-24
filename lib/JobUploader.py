import os
import time
import warnings
import fnmatch

import candidate_uploader
import diagnostic_uploader
import upload
import mailer
import jobtracker
import header
import config.upload
import config.basic

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
        """
        Drives the process of uploading results of the completed jobs.

        """
        #We might've crashed or stopped and couldn't upload checked
        self.upload_checked()
        self.upload_finished()

    def upload_checked(self):
        """
        Uploads checked results.

        """
        chk_query = "SELECT * FROM job_submits WHERE status='checked'"
        checked_submit = jobtracker.query(chk_query,fetchone=True)

        while checked_submit:
            if self.upload(checked_submit,commit=True):
            	self.clean_up(checked_submit)
            checked_submit = jobtracker.query(chk_query,fetchone=True)

    def upload_finished(self):
        fin_query = "SELECT * FROM job_submits WHERE status='finished'"
        finished_submit = jobtracker.query(fin_query,fetchone=True)

        while finished_submit:
            print "Uploading %s" % str(finished_submit['output_dir'])
            if self.upload(finished_submit,commit=False):
                print finished_submit
                if self.upload(finished_submit,commit=True):
                	self.clean_up(finished_submit)
            finished_submit = jobtracker.query(fin_query,fetchone=True)

    def upload(self,job_submit,commit=False):
        """
        Uploads Results for a given submit.

        Input(s):
            job_submit: dict representation of job_submits record
        Output(s):
            boolean True/False: whether upload/check succeeded
        """
        if(commit):
            check_or_upload='upload'
        else:
            check_or_upload='check'

        try:
            if commit:
                header_id = self.header_upload(job_submit,commit=commit)
            else:
                header_id = 1

            if header_id:
                if self.candidates_upload(job_submit,header_id=header_id,commit=commit):
                    if self.diagnostics_upload(job_submit,commit=commit):
                        if commit:
                           jobtracker.query("UPDATE job_submits SET status='uploaded' WHERE id=%u" % int(job_submit['id']))
                           jobtracker.query("UPDATE jobs SET status='uploaded' WHERE id=%u" % int(job_submit['job_id']))
                        else:
                           jobtracker.query("UPDATE job_submits SET status='checked' WHERE id=%u" % int(job_submit['id']))
                        return True

        except header.HeaderError, e:
            print "Header Uploader Parsing error: %s  \njobs.id: %u \tjob_submits.id:%u" % (str(e),int(job_submit['job_id']), int(job_submit['id']))

            jobtracker.query("UPDATE job_submits SET status='check_failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Header %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')) ,jobtracker.nowstr(), int(job_submit['id'])))

            jobtracker.query("UPDATE jobs SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Header %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')) ,jobtracker.nowstr(), int(job_submit['job_id'])))

            try:
                notification = mailer.ErrorMailer('Header %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False

        except candidate_uploader.PeriodicityCandidateError, e:
            print "Candidates Uploader Parsing error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_submit['job_id']), int(job_submit['id']))

            jobtracker.query("UPDATE job_submits SET status='check_failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),jobtracker.nowstr(), int(job_submit['id'])))

            jobtracker.query("UPDATE jobs SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),jobtracker.nowstr(), int(job_submit['job_id'])))

            try:
                notification = mailer.ErrorMailer('Candidates %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False

        except diagnostic_uploader.DiagnosticError, e:
            print "Diagnostics Uploader Parsing error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_submit['job_id']), int(job_submit['id']))

            jobtracker.query("UPDATE job_submits SET status='check_failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Diagnostics %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),jobtracker.nowstr(), int(job_submit['id'])))

            jobtracker.query("UPDATE jobs SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Diagnostics %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),jobtracker.nowstr(), int(job_submit['job_id'])))

            try:
                notification = mailer.ErrorMailer('Diagnostics %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False

        except NoRawResultFiles, e:
            print "%s" % str(e)

            jobtracker.query("UPDATE job_submits SET status='check_failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Header %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')) ,jobtracker.nowstr(), int(job_submit['id'])))

            jobtracker.query("UPDATE jobs SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Header %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),jobtracker.nowstr(), int(job_submit['job_id'])))

            try:
                notification = mailer.ErrorMailer('Header %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False

        except NoRawResultDir, e:
            print "%s" % str(e)

            jobtracker.query("UPDATE job_submits SET status='check_failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates/Diagnostics %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')) ,jobtracker.nowstr(), int(job_submit['id'])))

            jobtracker.query("UPDATE jobs SET status='failed', details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates/Diagnostics %s failed: %s' % (check_or_upload,str(e).replace("'","").replace('"','')),jobtracker.nowstr(), int(job_submit['job_id'])))
            try:
                notification = mailer.ErrorMailer('Candidates/Diagnostics %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False

        except upload.UploadError, e:
            print "Result Uploader error: %s  \njobs.id: %u \tjob_uploads.id:%u" % (str(e),int(job_submit['job_id']), int(job_submit['id']))
            jobtracker.query("UPDATE job_submits SET details='%s', updated_at='%s' WHERE id=%u"\
            % ('Result uploader error (probable connection issues)',jobtracker.nowstr(), int(job_submit['id'])))
            try:
                notification = mailer.ErrorMailer('Header %s failed: %s' % (check_or_upload,str(e)))
                notification.send()
            except Exception,e:
                pass
            return False
        return False


    def header_upload(self,job_submit,commit=False):
        """
        Uploads/Checks header for a processed job.

        Input(s):
            sqlite3.row job_row: represents an entry of related records in tables: jobs,job_uploads,job_submits
            boolean commit:
                True: Uploads header information
                False: Checks header information
        Output(s):
            boolean: True if operation succeeded.
                    False otherwise.
        """
    	dry_run = not commit;

        if(dry_run):
            check_or_upload='check'
        else:
            check_or_upload='upload'

        raw_files_for_header = self.get_raw_result_files(job_submit)
        if not raw_files_for_header:
            raise NoRawResultFiles("No *.fits files found in result directory for jobs.id: %u  job_submits.id:%u"\
                                    % (int(job_submit['job_id']), int(job_submit['id'])))

        header_id = header.upload_header(fns=raw_files_for_header,dry_run=dry_run)

        jobtracker.query("UPDATE job_submits SET details='%s', updated_at='%s' WHERE id=%u"\
                    % ('Header %s' % check_or_upload ,jobtracker.nowstr(), int(job_submit['id'])))

        if(dry_run):
            print "Header check success for jobs.id: %u \tjob_submits.id:%u" % (int(job_submit['job_id']), int(job_submit['id']))
            return True
        else:
            print "Header upload success for jobs.id: %u \tjob_submits.id:%u \theader_id: %u" % (int(job_submit['job_id']), int(job_submit['id']),int(header_id))
            return header_id

    def candidates_upload(self,job_submit,header_id=0,commit=False):
        """
        Uploads/Checks candidates for a processed job.

        Input(s):
            sqlite3.row job_row: represents an entry of related records in tables: jobs,job_uploads,job_submits
            boolean commit:
                True: Uploads candidates information
                False: Checks candidates information
        Output(s):
            boolean: True if operation succeeded.
                    False otherwise.
        """
        dry_run = not commit;

        if(dry_run):
            check_or_upload='check'
        else:
            check_or_upload='upload'


        dir = job_submit['output_dir']
        if not os.path.exists(dir):
            raise NoRawResultDir('The result directory [%s] for jobs.id: %u job_submits.id: %u was not found.'\
                                  % ( dir, int(job_submit['job_id']),int(job_submit['id'] )) )

        candidate_uploader.upload_candidates(header_id=header_id, versionnum=config.upload.version_num,  directory=dir,dry_run=dry_run)
        print "Candidates %s success for jobs.id: %u \tjob_uploads.id:%u" % (check_or_upload,int(job_submit['job_id']), int(job_submit['id']))
        jobtracker.query("UPDATE job_submits SET details='%s', updated_at='%s' WHERE id=%u"\
            % ('Candidates %s' % check_or_upload,jobtracker.nowstr(), int(job_submit['id'])))
        return True



    def diagnostics_upload(self,job_submit,commit=False):
        """
        Uploads/Checks diagnostics for a processed job.

        Input(s):
            sqlite3.row job_row: represents an entry of related records in tables: jobs,job_uploads,job_submits
            boolean commit:
                True: Uploads diagnostics information
                False: Checks diagnostics information
        Output(s):
            boolean: True if operation succeeded.
                    False otherwise.
        """
        dry_run = not commit;

        if(dry_run):
            check_or_upload='check'
        else:
            check_or_upload='upload'

        dir = job_submit['output_dir']
        if not os.path.exists(dir):
            raise NoRawResultDir('The result directory [%s] for jobs.id: %u job_submits.id: %u was not found.'\
                                  % ( dir, int(job_submit['job_id']),int(job_submit['id'] )) )

        obs_name = dir.split('/')[len(dir.split('/'))-3]
        beamnum = int(dir.split('/')[len(dir.split('/'))-2])
        print "obs_name: %s  beamnum: %s" % (obs_name,beamnum)

        diagnostic_uploader.upload_diagnostics(obsname=obs_name,beamnum=beamnum, versionnum=config.upload.version_num,  directory=dir,dry_run=dry_run)

        print "Diagnostics %s success for jobs.id: %u \tjob_uploads.id:%u" % (check_or_upload,int(job_submit['job_id']), int(job_submit['id']))
        jobtracker.query("UPDATE job_submits SET details='%s', updated_at='%s' WHERE id=%u"\
            % ('Diagnostics %s' % check_or_upload ,jobtracker.nowstr(), int(job_submit['id'])))
        return True

    def create_new_uploads(self):
        """
        Creates new job_uploads entries, for processed jobs, with status 'new'.

        Input(s):
            None
            database entries
        Output(s):Input(s):
            sqlite3.row job_row: represents an entry of related records in tables: jobs,job_uploads,job_submits
            None
            new database entries
        """

        print "Creating new upload entries..."
        jobs_with_no_uploads = jobtracker.query("SELECT * FROM jobs WHERE status='processed' AND id NOT IN (SELECT job_id FROM job_uploads WHERE job_uploads.status IN ('new','checked','uploaded','failed'))")
        print "%d new uploads to enter" % len(jobs_with_no_uploads)
        for job_row in jobs_with_no_uploads:
            jobtracker.query("INSERT INTO job_uploads (job_id, status, details, created_at, updated_at) VALUES(%u,'%s','%s','%s','%s')"\
                % (job_row['id'], 'new','Newly added upload',jobtracker.nowstr(),jobtracker.nowstr()))

    def get_jobs_last_successful_submit(self,job_id):
        lss = jobtracker.query("SELECT * FROM job_submits WHERE status='finished' AND job_id=%u ORDER BY id DESC LIMIT 1" % (int(job_id)),fetchone=True)
        return lss

    def get_raw_result_files(self,job_submit):
        raw_filenames = list()

        if os.path.exists(job_submit['output_dir']):
            for file in os.listdir(job_submit['output_dir']):
                if fnmatch.fnmatch(file, '*.fits'):
                   raw_filenames.append(os.path.join(job_submit['output_dir'],file))

        if raw_filenames == list():
            return False
        else:
            return raw_filenames

    def get_jobs_last_upload(self,job_id):
        lu = jobtracker.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % (int(job_id)),fetchone=True)
        return lu

    def check_new_uploads(self):
        """
        Checks job_uploads entries with status being 'new'.
        Updates the database entries whether results are checked and ready for upload, or job_upload failed
        """
        new_uploads = jobtracker.query("SELECT jobs.* FROM jobs,job_uploads WHERE job_uploads.status='new' AND jobs.id=job_uploads.job_id")
        for job_row in new_uploads:
            if self.header_upload(job_row):
                print "Header check passed"
                if self.candidates_upload(job_row):
                    print "Candidates check passed"
                    if self.diagnostics_upload(job_row):
                       print "Diagnostics check passed"
                       last_upload_try_id = jobtracker.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
                       jobtracker.query("UPDATE job_uploads SET status='checked' WHERE id=%u" % last_upload_try_id)

    def mark_reprocess_failed(self):
        """
        Marks failed jobs for associated job_uploads to be reprocessed by JobPooler
        """
        failed_upload_jobs = jobtracker.query("SELECT * FROM jobs WHERE id IN (SELECT job_id FROM job_uploads WHERE status='failed')")
        for job_row in failed_upload_jobs:
            last_upload_try_id = jobtracker.query("SELECT * FROM job_uploads WHERE job_id=%u ORDER BY id DESC LIMIT 1" % job_row['id'])[0]['id']
            self.mark_for_reprocessing(job_row['id'], last_upload_try_id)

    def mark_for_reprocessing(self,job_id, last_upload_id):
        """
        Marks job and job submit as failed (for reprocessing), sets status on job upload entry to reprocessing.

        """
        jobtracker.query("UPDATE jobs SET status='failed' WHERE id=%u" % (int(job_id)))
        jobtracker.query("UPDATE job_submits SET status='failed' WHERE job_id=%u" % (int(job_id)))
        jobtracker.query("UPDATE job_uploads SET status='reprocessing' WHERE id=%u" % (int(last_upload_id)))


    def clean_up(self,job_submit):
        """
        Deletes raw files for a given job_row.

        Input(s):
            sqlite3.row job_row: represents an entry of related records in tables: jobs,job_uploads,job_submits
        Output(s):
            stdout that the file was deleted.
        """
        downloads = jobtracker.query('SELECT downloads.* FROM job_files,downloads WHERE job_files.job_id=%u  AND job_files.file_id=downloads.id' % (job_submit['job_id']))
        for download in downloads:
            if config.basic.delete_rawdata and os.path.exists(download['filename']):
                os.remove(download['filename'])
                print "Deleted: %s" % download['filename']

    def get_processed_jobs(self):
        """
        Returns array of sqlite3.row(s) of processed jobs
        """
        return jobtracker.query("SELECT * FROM jobs WHERE status='processed'")

    def get_upload_attempts(self,job_row):
        """
        Returns array of sqlite3.row(s) of upload attempts for the given job_row

        Input(s):
            sqlite3.row job_row: represents an entry of related records in tables: jobs,job_uploads,job_submits
        Output(s):
            array of sqlite3.row(s) of upload attempts for the given job_row
        """
        return jobtracker.query("SELECT * FROM job_uploads WHERE job_id = %u" % int(job_row['id']))

    def get_jobs_files(self,job_row):
        """
        Returns list of files associated to a give job_row

        Input(s):
            sqlite3.row job_row: represents an entry of related records in tables: jobs,job_uploads,job_submits
        Output(s):
            array of file path strings associated with a job_row
        """

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
                if config.basic.delete_rawdata and os.path.exists(file_path):
                    os.remove(file_path)


class NoUploadAttempt(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)

class NoRawResultFiles(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)

class NoRawResultDir(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)

class NoSuccessfulSubmit(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)
