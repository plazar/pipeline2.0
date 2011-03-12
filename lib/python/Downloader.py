import os.path
import sys
import os
import shutil
import time
import re
import threading
import urllib2
import suds
import M2Crypto

import mailer
import OutStream
import datafile
import jobtracker
import CornellFTP
import pipeline_utils
import config.background
import config.download
import config.email
import config.basic

dlm_cout = OutStream.OutStream("Download Module", \
                        os.path.join(config.basic.log_dir, "downloader.log"), \
                        config.background.screen_output)


def recover():
    """Recover from crash/exit.
        Set all files with status='downloading' to 'unverified'.
        Also set all download_attempts with status 'downloading' to
        'aborted'.
    """
    queries = []
    queries.append("UPDATE files " \
                   "SET status='unverified', " \
                        "updated_at='%s', " \
                        "details='Recovering from crash/exit' "
                   "WHERE status='downloading'" % jobtracker.nowstr())
    queries.append("UPDATE download_attempts " \
                   "SET status='aborted', " \
                        "updated_at='%s', " \
                        "details='Recovering from crash/exit' "
                   "WHERE status='downloading'" % jobtracker.nowstr())
    jobtracker.query(queries)


def can_request_more():
    """Returns whether Downloader can request more restores.
        This is based on took disk space allowed for downloaded
        file, disk space available on the file system, and maximum
        number of active requests allowed.

    Inputs:
        None
    Output:
        can_request: A boolean value. True if Downloader can make a request.
                        False otherwise.
    """
    active_requests = jobtracker.query("SELECT * FROM requests " \
                                       "WHERE status='waiting'")
    numactive = len(active_requests)
    used = get_space_used()
    avail = get_space_available()
    
    can_request = (numactive < config.download.numrestores) and \
            (avail > config.download.min_free_space) and \
            (used < config.download.space_to_use)
    return can_request


def get_space_used():
    """Return space used by the download directory (config.download.temp)

    Inputs:
        None
    Output:
        used: Size of download directory (in bytes)
    """
    files = jobtracker.query("SELECT * FROM files")

    total_size = 0
    for file in files:
        if os.path.exists(file['filename']):
            total_size += file['size']
    return total_size


def get_space_available():
    """Return space available on the file system where files
        are being downloaded.
    
        Inputs:
            None
        Output:
            avail: Number of bytes available on the file system.
    """
    s = os.statvfs(os.path.abspath(config.download.temp))
    total = s.f_bavail*s.f_frsize
    return total


def run():
    """Perform a single iteration of the downloader's loop.
    """
    if can_request_more():
        print "Making a request"
        make_request()
    print "Checking active requests"
    check_active_requests()
    print "Starting downloads"
    start_downloads()
    print "Verifying files"
    verify_files()
    print "Recovering failed downloads"
    recover_failed_downloads()


def make_request():
    """Make a request for data to be restored by connecting to the
        web services at Cornell.
    """
    num_beams = 1
    web_service = suds.client.Client(config.download.api_service_url).service
    try:
        guid = web_service.Restore(username=config.download.api_username, \
                                   pw=config.download.api_password, \
                                   number=num_beams, bits=4, fileType='wapp')
    except urllib2.URLError, e:
        raise pipeline_utils.PipelineError("urllib2.URLError caught when " \
                                           "making a request for restore: %s" % \
                                           str(e))
    if guid == "fail":
        raise pipeline_utils.PipelineError("Request for restore returned 'fail'.")

    requests = jobtracker.query("SELECT * FROM requests " \
                             "WHERE guid='%s'" % guid)
    if requests:
        # Entries in the requests table exist with this GUID!?
        raise pipeline_utils.PipelineError("There are %d requests in the " \
                                           "job-tracker DB with this GUID %s" % \
                                           (len(requests), guid))

    jobtracker.query("INSERT INTO requests ( " \
                        "guid, " \
                        "created_at, " \
                        "updated_at, " \
                        "status, " \
                        "details) " \
                     "VALUES ('%s', '%s', '%s', '%s', '%s')" % \
                     (guid, jobtracker.nowstr(), jobtracker.nowstr(), 'waiting', \
                        'Newly created request'))
   
def check_active_requests():
    """Check for any requests with status='waiting'. If there are
        some, check if the files are ready for download.
    """
    active_requests = jobtracker.query("SELECT * FROM requests " \
                                       "WHERE status='waiting'")
    
    web_service = suds.client.Client(config.download.api_service_url).service
    for request in active_requests:
        location = web_service.Location(guid=request['guid'], \
                                        username=config.download.api_username, \
                                        pw=config.download.api_password)
        if location == "done":
            create_file_entries(request)
        else:
            query = "SELECT (julianday('%s')-julianday(created_at))*24 " \
                        "AS deltaT_hours " \
                    "FROM requests " \
                    "WHERE guid='%s'" % \
                        (jobtracker.nowstr(), request['guid'])
            row = jobtracker.query(query, fetchone=True)
            if row['deltaT_hours'] > config.download.request_timeout:
                dlm_cout.outs("Restore (%s) is over %d hr old " \
                                "and still not ready. Marking " \
                                "it as failed." % \
                        (myRestore.guid, config.download.request_timeout))
                jobtracker.query("UPDATE requests " \
                                 "SET status='failed', " \
                                    "details='Request took too long (> %d hr)', " \
                                    "updated_at='%s' " \
                                 "WHERE guid='%s'" % \
                    (config.download.request_timeout, jobtracker.nowstr(), \
                            request['guid']))


def create_file_entries(request):
    """Given a row from the requests table in the job-tracker DB
        check the FTP server for its files and create entries in
        the files table.

        Input:
            request: A row from the requests table.
        Outputs:
            None
    """
    cftp = CornellFTP.CornellFTP()
    files = cftp.get_files(request['guid'])
    
    total_size = 0
    num_files = 0
    queries = []
    for fn, size in files:
        # Check if file is from the phantom beam (beam 7)
        datafile_type = datafile.get_datafile_type([fn])
        parsedfn = datafile_type.fnmatch(fn)
        if parsedfn.groupdict().setdefault('beam', '-1') == '7':
            print "Ignoring beam 7 data: %s" % fn
            continue

        # Insert entry into DB's files table
        queries.append("INSERT INTO files ( " \
                            "request_id, " \
                            "remote_filename, " \
                            "filename, " \
                            "status, " \
                            "created_at, " \
                            "updated_at, " \
                            "size) " \
                       "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d)" % \
                       (request['id'], fn, os.path.join(config.download.temp, fn), \
                        'new', jobtracker.nowstr(), jobtracker.nowstr(), size))
        total_size += size
        num_files += 1

    if num_files:
        queries.append("UPDATE requests " \
                       "SET size=%d, " \
                            "updated_at='%s', " \
                            "status='finished', " \
                            "details='Request has been filled' " \
                       "WHERE id=%d" % \
                       (total_size, jobtracker.nowstr(), request['id']))
    else:
        queries.append("UPDATE requests " \
                       "SET updated_at='%s', " \
                            "status='failed', " \
                            "details='No files to download' " \
                       "WHERE id=%d" % \
                       (jobtracker.nowstr(), request['id']))
    jobtracker.query(queries)


def start_downloads():
    """Check for entries in the files table with status 'retrying'
        or 'new' and start the downloads.
    """
    todownload  = jobtracker.query("SELECT * FROM files " \
                                   "WHERE status='retrying'")
    todownload += jobtracker.query("SELECT * FROM files " \
                                   "WHERE status='new'")

    for file in todownload:
        if can_download():
            # Update file status and insert entry into download_attempts
            queries = []
            queries.append("UPDATE files " \
                           "SET status='downloading', " \
                                "details='Initiated download', " \
                                "updated_at='%s' " \
                            "WHERE id=%d" % \
                            (jobtracker.nowstr(), file['id']))
            queries.append("INSERT INTO download_attempts (" \
                                "status, " \
                                "details, " \
                                "updated_at, " \
                                "download_id) " \
                           "VALUES ('%s', '%s', '%s', %d)" % \
                           ('downloading', 'Initiated download', jobtracker.nowstr(), \
                                file['id']))
            insert_id = jobtracker.query(queries)
            attempt = jobtracker.query("SELECT * FROM download_attempts " \
                                       "WHERE id=%d" % insert_id, \
                                       fetchone=True)
    
            download(attempt)
            # DownloadThread(attempt).start()


def can_download():
    """Return true if another download can be initiated.
        False otherwise.

        Inputs:
            None
        Output:
            can_dl: A boolean value. True if another download can be
                    initiated. False otherwise.
    """
    downloading = jobtracker.query("SELECT * FROM files " \
                                   "WHERE status='downloading'")
    numdownload = len(downloading)
    used = get_space_used()
    avail = get_space_available()
    
    can_dl = (numdownload < config.download.numdownloads) and \
            (avail > config.download.min_free_space) and \
            (used < config.download.space_to_use)
    return can_dl 


def download(attempt):
    """Given a row from the job-tracker's download_attempts table,
        actually attempt the download.
    """
    file = jobtracker.query("SELECT * FROM files " \
                            "WHERE id=%d" % attempt['download_id'], \
                            fetchone=True)
    request = jobtracker.query("SELECT * FROM requests " \
                               "WHERE id=%d" % file['request_id'], \
                               fetchone=True)

    print "Initiating download of %s" % os.path.split(file['filename'])[-1]
    queries = []
    try:
        cftp = CornellFTP.CornellFTP()
        cftp.download(os.path.join(request['guid'], file['remote_filename']))
    except Exception, e:
        raise
        queries.append("UPDATE files " \
                       "SET status='failed', " \
                            "updated_at='%s', " \
                            "details='Download failed - %s' " \
                       "WHERE id=%d" % \
                       (jobtracker.nowstr(), str(e), file['id']))
        queries.append("UPDATE download_attempts " \
                       "SET status='download_failed', " \
                            "details='Download failed - %s', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % \
                       (str(e), jobtracker.nowstr(), attempt['id']))
    else:
        queries.append("UPDATE files " \
                       "SET status='unverified', " \
                            "updated_at='%s', " \
                            "details='Download is complete - File is unverified' " \
                       "WHERE id=%d" % \
                       (jobtracker.nowstr(), file['id']))
        queries.append("UPDATE download_attempts " \
                       "SET status='complete', " \
                            "details='Download is complete', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % \
                       (jobtracker.nowstr(), attempt['id']))
    jobtracker.query(queries)


def verify_files():
    """For all downloaded files with status 'unverify' verify the files.
    """
    toverify = jobtracker.query("SELECT * FROM files " \
                                "WHERE status='unverified'")

    for file in toverify:
        if os.path.exists(file['filename']):
            actualsize = os.path.getsize(file['filename'])
        else:
            actualsize = -1
        expectedsize = file['size']

        last_attempt_id = jobtracker.query("SELECT id " \
                                           "FROM download_attempts " \
                                           "WHERE download_id=%s " \
                                           "ORDER BY id DESC " % file['id'], \
                                           fetchone=True)[0]
                                                
        queries = []
        if actualsize == expectedsize:
            # Everything checks out!
            queries.append("UPDATE files " \
                           "SET status='downloaded', " \
                                "details='Download is complete and verified', " \
                                "updated_at='%s'" \
                           "WHERE id=%d" % \
                           (jobtracker.nowstr(), file['id']))
            queries.append("UPDATE download_attempts " \
                           "SET status='downloaded', " \
                                "details='Download is complete and verified', " \
                                "updated_at='%s'" \
                           "WHERE id=%d" % \
                           (jobtracker.nowstr(), last_attempt_id))
        else:
            # Boo... verification failed.
            queries.append("UPDATE files " \
                           "SET status='failed', " \
                                "details='Downloaded file failed verification', " \
                                "updated_at='%s'" \
                           "WHERE id=%d" % \
                           (jobtracker.nowstr(), file['id']))
            queries.append("UPDATE download_attempts " \
                           "SET status='verification_failed', " \
                                "details='Downloaded file failed verification', " \
                                "updated_at='%s'" \
                           "WHERE id=%d" % \
                           (jobtracker.nowstr(), last_attempt_id))
        jobtracker.query(queries)


def recover_failed_downloads():
    """For each entry in the job-tracker DB's files table
        check if the download can be retried or not.
        Update status and clean up, as necessary.
    """
    failed_files = jobtracker.query("SELECT * FROM files " \
                                   "WHERE status='failed'")

    for file in failed_files:
        attempts = jobtracker.query("SELECT * FROM download_attempts " \
                                    "WHERE download_id=%d" % file['id'])
        if len(attempts) < config.download.numretries:
            # download can be retried
            jobtracker.query("UPDATE files " \
                             "SET status='retrying', " \
                                  "updated_at='%s', " \
                                  "details='Download will be attempted again' " \
                             "WHERE id=%s" % \
                             (jobtracker.nowstr(), file['id']))
        else:
            # Abandon this file
            if os.path.exists(file['filename']):
                os.remove(file['filename'])
            jobtracker.query("UPDATE files " \
                             "SET status='terminal_failure', " \
                                  "updated_at='%s', " \
                                  "details='This file has been abandoned' " \
                             "WHERE id=%s" % \
                             (jobtracker.nowstr(), file['id']))

    
def status():
    """Print downloader's status to screen.
    """
    used = get_space_used()
    avail = get_space_available()
    allowed = config.download.space_to_use
    print "Space used by downloaded files: %.2f GB of %.2f GB (%.2f%%)" % \
            (used/1024.0**3, allowed/1024.0**3, 100.0*used/allowed)
    print "Space available on file system: %.2f GB" % (avail/1024.0**3)

    numwait = jobtracker.query("SELECT COUNT(*) FROM requests " \
                               "WHERE status='waiting'", \
                               fetchone=True)[0]
    numfail = jobtracker.query("SELECT COUNT(*) FROM requests " \
                               "WHERE status='failed'", \
                               fetchone=True)[0]
    print "Number of requests waiting: %d" % numwait
    print "Number of failed requests: %d" % numfail

    numdlactive = jobtracker.query("SELECT COUNT(*) FROM files " \
                                   "WHERE status='downloading'", \
                                   fetchone=True)[0]
    numdlfail = jobtracker.query("SELECT COUNT(*) FROM files " \
                                 "WHERE status='failed'", \
                                 fetchone=True)[0]
    print "Number of active downloads: %d" % numdlactive
    print "Number of failed downloads: %d" % numdlfail


class DownloadThread(threading.Thread):
    """A sub-class of threading.Thread to download restored
        file from Cornell.
    """
    def __init__(self, attempt):
        """DownloadThread constructor.
            
            Input:
                attempt: A row from the job-tracker's download_attempts table.

            Output:
                self: The DownloadThread object constructed.
        """
        super(DownloadThread, self).__init__()
        self.attempt = attempt

    def run(self):
        """Download data as a separate thread.
        """
        download(self.attempt)
