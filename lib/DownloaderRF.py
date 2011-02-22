import os.path
import sys
import os
import threading
import shutil
import time
import re
#import urllib2
from suds.client import Client as sudsClient
import datafile
import jobtracker
import mailer
import OutStream
import config.background
import config.download
import config.email
import CornellFTP
import M2Crypto

dlm_cout = OutStream.OutStream("Download Module",config.download.log_file_path, config.background.screen_output)
dl_cout = OutStream.OutStream("Download Module: d/l thread",config.download.log_file_path,config.background.screen_output)

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

        #if can create more restores then request new ones and add them to restores array
        while True:
            restore_entry = jobtracker.query("SELECT * FROM requests WHERE status NOT IN ('finished','failed')",fetchone=True)
            if self.can_request_more() or restore_entry:
                if restore_entry:
                    print "Found existing restore: %s" % restore_entry['guid']
                    myRestore = restore(num_beams=1,guid=restore_entry['guid'])
                    request = True
                else:
                    myRestore = restore(num_beams=1)
                    request = myRestore.request()

                myRestore = restore(1,guid='e96ea139361740eca91f0f82ed4d889f')
                #myRestore.create_downloads()
                #myRestore.create_downloads()



                #myRestore.getLocation()
                WebService =  sudsClient(config.download.api_service_url).service

                response = WebService.Location(guid='e96ea139361740eca91f0f82ed4d889f',username='mcgill',pw='palfa@Mc61!!')
                print response
                ftp = M2Crypto.ftpslib.FTP_TLS()
                ftp.connect(config.download.ftp_host,config.download.ftp_port)
                ftp.auth_tls()
                ftp.set_pasv(1)
                print config.download.ftp_username,config.download.ftp_password
                login_response = ftp.login(config.download.ftp_username,config.download.ftp_password)
                print login_response
                exit()
                myRestore.create_downloads()
                myRestore.create_downloads()
                if request:
                    having_location = False
                    while not having_location:
                        print "Asking for location."
                        having_location = myRestore.getLocation()
                        if not having_location:
                            time.sleep(5)

                    myRestore.create_downloads()
#                    if downloads_created:
#                        print "Downlaods created. Starting Download."
#                        if myRestore.download_files():
#                            print "Mark Requests as downloaded"
#                    else:
#                        print "Fail this request"

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

        active_resquests = jobtracker.query("SELECT * FROM requests WHERE status NOT IN ('failed','retrieved_files')")
        if len(active_resquests) >= config.download.numrestores:
            dlm_cout.outs("Cannot have more than "+ str(config.download.numrestores) +" at a time.")
            return False

        total_size = 0
        for request in active_resquests:
            if request['size']:
                total_size += int(request['size'])

        dlm_cout.outs("Total estimated size of currently running restores: %u" % total_size)
        return ((self.get_available_space() - total_size) > 0)

    def get_available_space(self):
        """
        Returns space available to the Downloader

        Input(s):
            None
        Output(s):
            int : size in bytes available for Downloader storage
        """

        folder_size = 0
        if config.download.temp == "":
            print "Getting filename"
            path_to_size = os.path.dirname(__file__)
        else:
            print "Setting to config.download.temp"
            path_to_size = config.download.temp
        print path_to_size
        for (path, dirs, files) in os.walk(path_to_size):
          for file in files:
            try:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
            except Exception, e:
                dlm_cout.outs('There was an error while getting the file size: %s   Exception: %s' % (file,str(e)))
        return (config.download.space_to_use - folder_size)


class restore:
    """
    Class representing an active restore request.
    """

    def __init__(self,num_beams,guid=False):
        self.values = None
        self.num_beams = num_beams
        self.downloaders = dict()
        self.username = config.download.api_username
        self.password = config.download.api_password

        self.files = dict()
        self.size = 0
        self.guid = guid
        if self.guid:
            self.update_from_db()

    def create_downloads(self):
        C_FTP = CornellFTP.CornellFTP()
        print self.guid
        files = C_FTP.get_files('e96ea139361740eca91f0f82ed4d889f')
        if files:
            for filename,filesize in files:
                #existing_file = jobtracker.query("SELECT * FROM downloads WHERE remote_filename='%s'" % filename)
                print existing_file
                if not existing_file:
                    pass
                    #ins_query = "INSERT INTO downloads (request_id,remote_filename,filename,status,created_at,updated_at,size) VALUES ('%s','%s','%s','%s','%s','%s',%u)" % (self.values['id'],filename,os.path.join(config.download.temp,filename),'new',jobtracker.nowstr(),jobtracker.nowstr(), int(filesize))
                    #jobtracker.query(ins_query)

#    def download_files(self):
#        downloads = jobtracker.query("SELECT * FROM downloads WHERE request_id=%u AND status NOT 'downloaded'" % self.values['id'])
#        for download in downloads:
#            attempt_id = jobtracker.query("INSERT INTO download_attempts (download_id, status, created_at, updated_at) VALUES  ('%s','%s','%s')" % (download['id'],'downloading',jobtracker.nowstr(), jobtracker.nowstr() ))
#            try:
#                C_FTP = CornellFTP.CornellFTP()
#                C_FTP.download(os.path.join(self.guid,download['remote_filename']))
#                jobtracker.query("UPDATE download_attempts SET status='downloaded' WHERE id=%u" % int(attempt_id))
#                print "Download of %s SUCCEEDED." % download['remote_filename']
#            except Exception, e:
#                print "Download of %s FAILED." % download['remote_filename']
#                jobtracker.query("UPDATE download_attempts SET status='failed' WHERE id=%u" % int(attempt_id))

#    def request(self):
#        """
#        Requests a restore from Cornell's SOAP Webservice.
#
#        Input(s):
#            None
#        Output(s)
#            boolean False: if the API reported failure upon requesting a restore
#            string: guid of a restore to be tracked for availability of restore files.
#        """
#        dlm_cout.outs("Requesting Restore")
#        try:
#            response = self.WebService.Restore(username=self.username,pw=self.password,number=self.num_beams,bits=4,fileType="wapp")
#        except urllib2.URLError, e:
#            dlm_cout.outs("There was a problem requesting the restore. Reason: %s" % str(e))
#            return False
#        if response != "fail":
#            self.guid = response
#            if self.get_by_guid(self.guid) != list():
#                dlm_cout.outs("The record with GUID = '%s' allready exists" % (self.guid))
#            else:
#                self.create()
#                return response
#        else:
#            dlm_cout.outs("Failed to receive proper GUID", OutStream.OutStream.WARNING)
#            return False

#    def create(self):
#        insert_query = "INSERT INTO requests (guid, created_at, updated_at, status, details) VALUES ('%s','%s', '%s', '%s','%s')" % (self.guid, jobtracker.nowstr(), jobtracker.nowstr(), 'waiting', 'Newly created restore request')
#        jobtracker.query(insert_query)
#
#    def is_downloading(self):
#        """
#        Returns whether restore is downloading files.
#
#        Input(s):
#            None
#        Output(s):
#            boolean True: if regetLocationstore is downloading atleast one file.
#            boolean False: if restore is not downloading at all.
#        """
#        if self.downloaders == dict():
#            return False
#        else:
#            atleast_one = False
#            for filename in self.downloaders:
#                if self.downloaders[filename].is_alive():
#                    atleast_one = True
#        return atleast_one


    def getLocation(self):
        """
        Returns whether files for this restore were written to guid directory

        Input(s):
            none
        Output(s):
            boolean True: if files were written for this restore
            boolean False: if files are not yet ready for this restore.
        """
        WebService =  sudsClient(config.download.api_service_url).service

        response = WebService.Location(guid=self.guid,username='mcgill',pw='palfa@Mc61!!')
#        print response
        return True
#        for key,value in globals().items():
#            print key
#            print value
#            print "========"

        del(WebService)
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