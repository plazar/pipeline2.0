import os.path
import sys
import os
import shutil
import time
import re
import urllib2
import suds
import M2Crypto

import mailer
import OutStream
import datafile
import jobtracker
import CornellFTP
import config.background
import config.download
import config.email
import config.basic

dlm_cout = OutStream.OutStream("Download Module", \
                        os.path.join(config.basic.log_dir, "downloader.log"), \
                        config.background.screen_output)

class DownloadModule:
    """
    Allows requesting and downloading of restores through a SOAP api from Cornell University.
    """

    def __init__(self):
        dlm_cout.outs('Initializing Module')
        self.username = config.download.api_username
        self.password = config.download.api_password

    def run(self):
        """
        Drives status changes, requests for restores

        Input(s):
            None
        Output(s):
            None
        """

        while True:
            restore_entry = jobtracker.query("SELECT * FROM requests WHERE status NOT IN ('finished','failed')",fetchone=True)
            if self.can_request_more() or restore_entry:
                if restore_entry:
                    dlm_cout.outs("Found existing restore: %s" % restore_entry['guid'])
                    myRestore = restore(num_beams=1,guid=restore_entry['guid'])
                    request = True
                else:
                    myRestore = restore(num_beams=1)
                    request = myRestore.request()

                if request:
                    having_location = False
                    while not having_location:
                        having_location = myRestore.getLocation()
                        dlm_cout.outs("Files are ready for restore %s: %s" % (str(myRestore.guid), str(having_location) ))
                        if not having_location:
                            time.sleep(5)

                    downloads_created = myRestore.create_downloads()
                    dlm_cout.outs("Num. of Created Downloads for %s: %s" % (myRestore.values['guid'],str(downloads_created)))
                    if downloads_created:
                        if myRestore.download_files():
                            dlm_cout.outs("Marking Request as downloaded: %s" % myRestore.values['guid'])
                            update_query = "UPDATE requests SET status='finished' WHERE id=%u" % int(myRestore.values['id'])
                            jobtracker.query(update_query)
                    else:
                        dlm_cout.outs("Marking Request as failed: %s" % myRestore.values['guid'])
                        update_query = "UPDATE requests SET status='failed' WHERE id=%u" % int(myRestore.values['id'])
                        jobtracker.query(update_query)
            time.sleep(config.background.sleep)

    def get_space_used(self):
        """
        Reports space used by the download directory (config.download.temp)

        Input(s):
            None
        Output(s):
            int: size of download directory (config.download.temp)
        """
        folder_size = 0
        for (path, dirs, files) in os.walk(config.download.temp):
          for file in files:
            try:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
            except Exception, e:
                dlm_cout.outs('There was an error while getting the file size: %s   Exception: %s' % (filename,str(e)) )
        return folder_size

    def have_space(self):
        """
        Returns if Downloader has device space left for more storage.

        Input(s):
            None
        Output(s):
            boolean True: if Downloader has device space to use.
            boolean False: if Downloader has no more space to use.
        """

        folder_size = self.get_space_used()
        if folder_size < config.download.space_to_use:
            return True
        else:
            return False

    def can_request_more(self):
        """
        Returns whether Downloader can request more restores.

        Input(s):
            None
        Output(s):
            boolean True: if Downloader can request more restores.
            boolean False: if Downloader may not request more restores.
        """

        active_requests = jobtracker.query("SELECT * FROM requests WHERE status IN ('waiting','ready')")
        if len(active_requests) >= config.download.numrestores:
            dlm_cout.outs("Cannot have more than "+ str(config.download.numrestores) +" at a time.")
            return False

        total_size = 0
        for request in active_requests:
            if request['size']:
                total_size += int(request['size'])

        dlm_cout.outs("Total estimated size of currently running restores: %s GB" % (total_size/1024**3) )
        return ((self.get_available_space() - total_size) > 0)

    def get_available_space(self):
        """
        Returns space available to the Downloader

        Input(s):
            None
        Output(s):
            int : size in bytes available for Downloader storage
        """

        dlm_cout.outs("Calculating available space...")
        folder_size = 0
        if config.download.temp == "":
            path_to_size = os.path.dirname(__file__)
        else:
            path_to_size = config.download.temp
        for (path, dirs, files) in os.walk(path_to_size):
          for file in files:
            try:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
            except Exception, e:
                dlm_cout.outs('There was an error while getting the file size: %s   Exception: %s' % (file,str(e)))
        dlm_cout.outs("Calculated space available: %s GB" % str( (config.download.space_to_use - folder_size )/1024**3 ))
        return (config.download.space_to_use - folder_size)


class restore:
    """
    Class representing an active restore request.
    """

    def __init__(self,num_beams,guid=False):
        self.values = None
        self.num_beams = num_beams
        self.downloaders = dict()
        self.WebService =  suds.client.Client(config.download.api_service_url).service
        self.username = config.download.api_username
        self.password = config.download.api_password

        self.files = dict()
        self.size = 0
        self.guid = guid
        if self.guid:
            self.update_from_db()

    def create_downloads(self):
        self.update_from_db()
        dlm_cout.outs("Creating/Updating Download entries for %s:" % self.guid)
        C_FTP = CornellFTP.CornellFTP()
        try:
            files = C_FTP.get_files(self.guid)
        except Exception, e:
            dlm_cout.outs("There was an error while getting the files listing for: %s" % self.guid)
            return False
        total_size = 0
        num_of_downloads = 0
        if files:
            for filename,filesize in files:
                existing_file = jobtracker.query("SELECT * FROM downloads WHERE remote_filename='%s'" % filename)
                if not existing_file:
                    datafile_type = datafile.get_datafile_type([filename])
                    parsedfn = datafile_type.fnmatch(filename)
                    if parsedfn.groupdict().setdefault('beam', '-1') != '7':
                        total_size += filesize
                        ins_query = "INSERT INTO downloads (request_id,remote_filename,filename,status,created_at,updated_at,size) VALUES ('%s','%s','%s','%s','%s','%s',%u)" % (self.values['id'],filename,os.path.join(config.download.temp,filename),'new',jobtracker.nowstr(),jobtracker.nowstr(), int(filesize))
                        jobtracker.query(ins_query)
                        num_of_downloads += 1
                    else:
                        dlm_cout.outs(self.guid +" IGNORING: %s" % filename)
                else:
                    total_size += filesize
                    num_of_downloads += 1
            update_query = "UPDATE requests SET size=%u WHERE id=%u" % ( int(total_size), int(self.values['id']) )
            jobtracker.query(update_query)
            return num_of_downloads
        else:
            return False

    def download_files(self):
        downloads = jobtracker.query("SELECT * FROM downloads WHERE request_id=%u AND status != 'downloaded'" % int(self.values['id']))
        download_failed = False
        for download in downloads:
            jobtracker.query("UPDATE download_attempts SET status='failed' WHERE download_id=%u" % int(download['id']))
            jobtracker.query("UPDATE downloads SET status='downloading' WHERE id=%u" % int(download['id']))
            redownload = self.can_redownload(download)
            while redownload:
                attempt_id = jobtracker.query("INSERT INTO download_attempts (download_id, status, created_at, updated_at) VALUES  ('%s','%s','%s','%s')" % (download['id'],'downloading',jobtracker.nowstr(), jobtracker.nowstr() ))
                try:
                    C_FTP = CornellFTP.CornellFTP()
                    C_FTP.download(os.path.join(self.guid,download['remote_filename']))
                    jobtracker.query("UPDATE downloads SET status='downloaded' WHERE id=%u" % int(download['id']))
                    jobtracker.query("UPDATE download_attempts SET status='downloaded' WHERE id=%u" % int(attempt_id))
                    dlm_cout.outs("Download of %s COMPLETED." % download['remote_filename'])
                    redownload = False
                except Exception, e:
                    dlm_cout.outs("Download of %s FAILED." % download['remote_filename'])
                    jobtracker.query("UPDATE download_attempts SET status='failed' WHERE id=%u" % int(attempt_id))
                    redownload = self.can_redownload(download)
                    if not redownload:
                        download_failed = True
        return not download_failed

    def can_redownload(self,download_entry):
        sel_query = "SELECT * FROM download_attempts WHERE download_id=%u" % download_entry['id']
        download_attempts = jobtracker.query(sel_query)
        whether_redownload = config.download.numretries > len(download_attempts)
        dlm_cout.outs( "Whether Download %s : %s" % (download_entry['remote_filename'], whether_redownload))
        return whether_redownload

    def request(self):
        """
        Requests a restore from Cornell's SOAP Webservice.

        Input(s):
            None
        Output(s)
            boolean False: if the API reported failure upon requesting a restore
            string: guid of a restore to be tracked for availability of restore files.
        """
        dlm_cout.outs("Requesting Restore")
        try:
            response = self.WebService.Restore(username=self.username,pw=self.password,number=self.num_beams,bits=4,fileType="wapp")
        except urllib2.URLError, e:
            dlm_cout.outs("There was a problem requesting the restore. Reason: %s" % str(e))
            return False
        if response != "fail":
            self.guid = response
            if self.get_by_guid(self.guid) != list():
                dlm_cout.outs("The record with GUID = '%s' allready exists" % (self.guid))
            else:
                self.create()
                return response
        else:
            dlm_cout.outs("Failed to receive proper GUID", OutStream.OutStream.WARNING)
            return False

    def create(self):
        insert_query = "INSERT INTO requests (guid, created_at, updated_at, status, details) VALUES ('%s','%s', '%s', '%s','%s')"\
         % (self.guid, jobtracker.nowstr(), jobtracker.nowstr(), 'waiting', 'Newly created restore request')
        jobtracker.query(insert_query)

    def getLocation(self):
        """
        Returns whether files for this restore were written to guid directory

        Input(s):
            none
        Output(s):
            boolean True: if files were written for this restore
            boolean False: if files are not yet ready for this restore.
        """
        response = self.WebService.Location(guid=self.guid,username=self.username,pw=self.password)
        if response == "done":
            jobtracker.query("UPDATE requests SET status = 'ready' WHERE guid ='%s'" % (self.guid))
            return True
        else:
            return False

    def downloaded_size_match(self,attempt_id):
        """
        Verifies downloaded file's size and file size on the FTP matches for a given download_attempts id

        Input(s):
            int attempt_id: current download_attempts id for this downloads of this restore
        Output(s):
            boolean True: if ftp listing size matches downloaded file size.
            boolean False: if ftp listing size does not match downloaded file size.
        """

        attempt_row = jobtracker.query("SELECT * FROM download_attempts WHERE id=%u" % int(attempt_id))[0]
        download = jobtracker.query("SELECT * FROM downloads WHERE id=%u" % int(attempt_row['download_id']))[0]

        if os.path.exists(download['filename']):
            return (os.path.getsize(download['filename']) == int(download['size']))
        else:
            dlm_cout.outs("Does not exist: %s" % download['filename'])
            return False


    def status(self):
        """
        Reports summary of a current restore.
            restore GUID , files being downlaoded for this restore and their expected size.
        """

        dls = jobtracker.query("SELECT * from downloads WHERE request_id = %s" % self.values['id'])
        print "Restore: %s" % self.guid
        print "\t\tDownloading: "
        for dl in dls:
            print "\t\t %s \t[%s]" % (dl['remote_filename'],str(dl['size']))

    def is_finished(self):
        """
        Tests whether this restore successfully completed downlaoding all of the associated files.

        Input(s):
            None
        Output(s):
            boolean True: if this restore successfully downlaoded all of its associated files.
            boolean False: if this restore failed to download all of its associated files.
        """

        all_downloads = jobtracker.query("SELECT * FROM downloads WHERE request_id = %s" % self.values['id'])
        finished_downloads = jobtracker.query("SELECT * FROM downloads WHERE request_id = %s AND status LIKE 'downloaded'" % self.values['id'])
        failed_downloads = jobtracker.query("SELECT * FROM downloads WHERE request_id = %s AND status LIKE 'failed'" % self.values['id'])
        downloading = jobtracker.query("SELECT * FROM downloads WHERE request_id = %s AND status LIKE 'downloading'" % self.values['id'])

        if len(downloading) > 0:
            return False

        if len(all_downloads) == 0 and self.downloaders == dict():
            return False

        if len(all_downloads) == len(finished_downloads):
            jobtracker.query("UPDATE requests SET status ='finished', updated_at='%s' WHERE id = %s" % (jobtracker.nowstr(), self.values['id']))
            return True

        for failed_download in failed_downloads:
            number_of_attempts = len(jobtracker.query("SELECT * FROM download_attempts WHERE download_id = %s" % failed_download['id']))
            if config.download.numretries < number_of_attempts:
                return False

        jobtracker.query("UPDATE requests SET status ='finished', updated_at='%s' WHERE id=%s" % (jobtracker.nowstr(),self.values['id']) )
        return True

    def update_from_db(self):
        """
        Updates this restores vaules from the database.
        """
        self.values = self.get_by_guid(self.guid)

    def get_by_guid(self, guid):
        """
        Returns sqlite3.row of an entry matching given guid.

        Input(s):
            string guid: unique restore identifier
        Output(s):
            sqlite3.row: if the restore entry was found
            empty list(): if restore entry was not found
        """
        result = jobtracker.query("SELECT * FROM requests WHERE guid = '%s'" % guid)
        if result == list():
            return result
        return result[0]
